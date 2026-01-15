/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for LeveledCompactionStrategy's unsafe_aggressive_sstable_expiration feature.
 */
public class LCSAggressiveExpirationTest extends CQLTester
{
    /**
     * Tests that tombstones are NOT purged when there are overlapping SSTables
     * and unsafe_aggressive_sstable_expiration is disabled (default behavior).
     *
     * Setup:
     * - SSTable 1: Data for key "key1" with LOW timestamp
     * - SSTable 2: Tombstone for key "key1" with HIGH timestamp (tombstone wins)
     *
     * Without aggressive expiration (ignoreOverlaps=false), the tombstone should NOT
     * be purged because there's overlapping data in SSTable 1. The row should remain
     * deleted after compaction.
     *
     * This is the inverse of testDataResurrectionWithAggressiveExpiration which shows
     * that WITH aggressive expiration, the tombstone IS purged and data resurrects.
     */
    @Test
    public void testTombstoneNotPurgedWithOverlapsDefault() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, data text) " +
                    "WITH compaction = {'class': 'LeveledCompactionStrategy', 'enabled': 'false'} " +
                    "AND gc_grace_seconds = 0");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        long timestamp = System.currentTimeMillis() * 1000; // microseconds

        // SSTable 1: Data with LOW timestamp
        execute("INSERT INTO %s (id, data) VALUES ('key1', 'old_value') USING TIMESTAMP ?", timestamp - 2000000);
        flush();
        SSTableReader sstableWithData = cfs.getLiveSSTables().iterator().next();

        // SSTable 2: Tombstone with HIGH timestamp (tombstone > data, so tombstone wins)
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE id = 'key1'", timestamp - 1000000);
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        // Find the SSTable with the tombstone
        SSTableReader sstableWithTombstone = null;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable != sstableWithData && hasTombstones(sstable))
            {
                sstableWithTombstone = sstable;
                break;
            }
        }
        assertNotNull("Should have an SSTable with tombstones", sstableWithTombstone);

        // Before compaction: tombstone wins, row should be deleted
        assertEmpty(execute("SELECT * FROM %s WHERE id = 'key1'"));

        // Compact ONLY the tombstone SSTable with ignoreOverlaps=false (default behavior)
        Set<SSTableReader> toCompact = Sets.newHashSet(sstableWithTombstone);

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION))
        {
            if (txn != null)
            {
                LeveledCompactionTask task = new LeveledCompactionTask(cfs, txn, 0,
                    Integer.MAX_VALUE, // Far future - ensures tombstones are old enough to purge
                    160 * 1024 * 1024L, false, false); // ignoreOverlaps = false
                task.execute(null);
            }
        }

        // After compaction: tombstone should NOT be purged (because ignoreOverlaps=false
        // and there's overlapping data in sstableWithData), so row should STILL be deleted
        assertEmpty(execute("SELECT * FROM %s WHERE id = 'key1'"));
    }

    /**
     * Tests that tombstones ARE purged when unsafe_aggressive_sstable_expiration is enabled,
     * even when there are overlapping SSTables.
     */
    @Test
    public void testTombstonePurgedWithAggressiveExpiration() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, data text) " +
                    "WITH compaction = {'class': 'LeveledCompactionStrategy', " +
                    "'unsafe_aggressive_sstable_expiration': 'true', 'enabled': 'false'} " +
                    "AND gc_grace_seconds = 0");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (id, data) VALUES ('target', 'original')");
        flush();
        
        execute("DELETE FROM %s WHERE id = 'target'");
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        SSTableReader sstableWithTombstone = null;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (hasTombstones(sstable))
            {
                sstableWithTombstone = sstable;
                break;
            }
        }
        assertNotNull("Should have an SSTable with tombstones", sstableWithTombstone);

        Set<SSTableReader> toCompact = Sets.newHashSet(sstableWithTombstone);

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION))
        {
            if (txn != null)
            {
                LeveledCompactionTask task = new LeveledCompactionTask(cfs, txn, 0,
                    Integer.MAX_VALUE, // Far future - ensures tombstones are old enough to purge
                    160 * 1024 * 1024L, false, true); // ignoreOverlaps = true
                task.execute(null);
            }
        }

        // After aggressive compaction, the tombstone SSTable should be gone or empty
        // and the remaining data shouldn't have the tombstone anymore
        boolean foundTombstone = false;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (hasTombstones(sstable))
            {
                foundTombstone = true;
                break;
            }
        }
        assertFalse("Tombstones should have been purged with aggressive expiration", foundTombstone);
    }

    /**
     * Tests the full scenario: with overlapping data, aggressive expiration causes
     * "deleted" data to resurrect because the tombstone is purged.
     */
    @Test
    public void testDataResurrectionWithAggressiveExpiration() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, data text) " +
                    "WITH compaction = {'class': 'LeveledCompactionStrategy', " +
                    "'unsafe_aggressive_sstable_expiration': 'true', 'enabled': 'false'} " +
                    "AND gc_grace_seconds = 0");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        long timestamp = System.currentTimeMillis() * 1000; // microseconds

        execute("INSERT INTO %s (id, data) VALUES ('key1', 'old_value') USING TIMESTAMP ?", timestamp - 2000000);
        flush();
        SSTableReader sstable1 = cfs.getLiveSSTables().iterator().next();

        execute("DELETE FROM %s USING TIMESTAMP ? WHERE id = 'key1'", timestamp - 1000000);
        flush();

        SSTableReader sstableWithTombstone = null;
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            if (sstable != sstable1 && hasTombstones(sstable))
            {
                sstableWithTombstone = sstable;
                break;
            }
        }
        assertNotNull("Should have an SSTable with tombstones", sstableWithTombstone);

        // At this point, reading should return nothing (tombstone wins)
        assertEmpty(execute("SELECT * FROM %s WHERE id = 'key1'"));

        // Compact ONLY the tombstone SSTable with aggressive expiration
        Set<SSTableReader> toCompact = Sets.newHashSet(sstableWithTombstone);

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION))
        {
            if (txn != null)
            {
                LeveledCompactionTask task = new LeveledCompactionTask(cfs, txn, 0,
                    Integer.MAX_VALUE, // Far future - ensures tombstones are old enough to purge
                    160 * 1024 * 1024L, false, true); // ignoreOverlaps = true
                task.execute(null);
            }
        }

        // DATA RESURRECTION: The tombstone was purged but the old data remains in sstable1
        // Now the "deleted" data is visible again!
        assertRows(execute("SELECT data FROM %s WHERE id = 'key1'"), row("old_value"));
    }

    /**
     * Tests that invalid values for the option are rejected.
     */
    @Test
    public void testInvalidOptionValue() throws Throwable
    {
        try
        {
            createTable("CREATE TABLE %s (id text PRIMARY KEY, data text) " +
                        "WITH compaction = {'class': 'LeveledCompactionStrategy', " +
                        "'unsafe_aggressive_sstable_expiration': 'invalid'}");
            fail("Should have rejected invalid option value");
        }
        catch (RuntimeException e)
        {
            // CQLTester wraps ConfigurationException in RuntimeException
            assertTrue("Exception should be caused by ConfigurationException",
                       e.getCause() instanceof ConfigurationException);
            assertTrue("Error message should mention unsafe_aggressive_sstable_expiration",
                       e.getCause().getMessage().contains("unsafe_aggressive_sstable_expiration"));
        }
    }

    /**
     * Tests that fully expired SSTables are dropped with aggressive expiration
     * even when there are overlapping SSTables (similar to TWCS behavior).
     *
     * Setup:
     * - SSTable 1: Data with TTL (will expire)
     * - SSTable 2: Non-expiring data with overlapping key
     *
     * With aggressive expiration enabled, the expired SSTable should be dropped
     * even though there's overlapping data in SSTable 2.
     */
    @Test
    public void testDropOverlappingExpiredSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                    "WITH compaction = {'class': 'LeveledCompactionStrategy', 'enabled': 'false'} " +
                    "AND gc_grace_seconds = 0 " +
                    "AND default_time_to_live = 1");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // SSTable 1: Expiring data (1 second TTL from table default)
        execute("INSERT INTO %s (id, data) VALUES (1, 'expiring')");
        flush();
        SSTableReader expiredSSTable = cfs.getLiveSSTables().iterator().next();

        // SSTable 2: Non-expiring data with overlapping key (TTL 0 = no expiration)
        execute("INSERT INTO %s (id, data) VALUES (1, 'permanent') USING TTL 0");
        execute("INSERT INTO %s (id, data) VALUES (2, 'other') USING TTL 0");
        flush();
        assertEquals(2, cfs.getLiveSSTables().size());

        // With aggressive expiration enabled, compact the expired SSTable
        Set<SSTableReader> toCompact = Sets.newHashSet(expiredSSTable);

        try (LifecycleTransaction txn = cfs.getTracker().tryModify(toCompact, OperationType.COMPACTION))
        {
            if (txn != null)
            {
                // ignoreOverlaps = true (aggressive expiration behavior)
                LeveledCompactionTask task = new LeveledCompactionTask(cfs, txn, 0,
                    Integer.MAX_VALUE, // Far future - ensures TTL data is old enough to purge
                    160 * 1024 * 1024L, false, true); // ignoreOverlaps = true
                task.execute(null);
            }
        }

        // After compaction: the expired SSTable should be dropped (fully expired data removed)
        assertEquals("Should have only 1 SSTable after dropping expired one",
                     1, cfs.getLiveSSTables().size());
        assertFalse("Expired SSTable should no longer exist",
                    cfs.getLiveSSTables().contains(expiredSSTable));

        // Verify the permanent data is still accessible
        assertRows(execute("SELECT data FROM %s WHERE id = 1"), row("permanent"));
        assertRows(execute("SELECT data FROM %s WHERE id = 2"), row("other"));
    }

    /**
     * Helper method to check if an SSTable contains tombstones.
     */
    private boolean hasTombstones(SSTableReader sstable)
    {
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator iter = scanner.next())
                {
                    if (!iter.partitionLevelDeletion().isLive())
                        return true;
                    while (iter.hasNext())
                    {
                        Unfiltered unfiltered = iter.next();
                        if (unfiltered.isRow())
                        {
                            Row row = (Row) unfiltered;
                            if (!row.deletion().isLive())
                                return true;
                            for (Cell<?> c : row.cells())
                            {
                                if (c.isTombstone())
                                    return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }
}

