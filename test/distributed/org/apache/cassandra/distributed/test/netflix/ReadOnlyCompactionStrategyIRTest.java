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

package org.apache.cassandra.distributed.test.netflix;

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Distributed tests for ReadOnlyCompactionStrategy with incremental repair.
 */
public class ReadOnlyCompactionStrategyIRTest extends TestBaseImpl
{
    private static final String ROCS = "com.netflix.cassandra.db.compaction.ReadOnlyCompactionStrategy";

    @Test
    public void testIncrementalRepairWithROCS() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .start()))
        {
            String table = "tbl";
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + "." + table + " (k int PRIMARY KEY, v int)" +
                                 " WITH compaction = {'class': '" + ROCS + "'}");

            // Insert data and flush to create SSTables on both nodes
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + table + " (k, v) VALUES (?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            cluster.forEach(node -> node.flush(KEYSPACE));

            // Verify SSTables exist and are unrepaired before repair
            cluster.forEach(node -> node.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                Assert.assertFalse("Should have SSTables before repair", cfs.getLiveSSTables().isEmpty());
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    Assert.assertFalse("SSTables should be unrepaired before IR", sstable.isRepaired());
            }));

            // Run incremental repair (default, no --full flag)
            NodeToolResult result = cluster.get(1).nodetoolResult("repair", KEYSPACE, table);
            result.asserts().success();

            // Wait for SSTables to be marked repaired (async anti-compaction)
            cluster.forEach(node -> node.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                for (int attempt = 0; attempt < 100; attempt++)
                {
                    if (cfs.getLiveSSTables().stream().allMatch(SSTableReader::isRepaired))
                        return;
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
                Assert.fail("SSTables were not all marked repaired after IR");
            }));

            // Verify data is still readable after IR
            Object[][] rows = cluster.coordinator(1).execute("SELECT count(*) FROM " + KEYSPACE + "." + table,
                                                              ConsistencyLevel.ALL);
            Assert.assertEquals(100L, rows[0][0]);
        }
    }

    @Test
    public void testBackgroundCompactionAfterIncrementalRepair() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .start()))
        {
            String table = "tbl2";
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + "." + table + " (k int PRIMARY KEY, v int)" +
                                 " WITH compaction = {'class': '" + ROCS + "'}");

            // Insert data in multiple rounds to create overlapping SSTables
            for (int round = 0; round < 3; round++)
            {
                for (int i = 0; i < 50; i++)
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + table + " (k, v) VALUES (?, ?)",
                                                   ConsistencyLevel.ALL, i + (round * 50), i);
                cluster.forEach(node -> node.flush(KEYSPACE));
            }

            // Run incremental repair
            NodeToolResult result = cluster.get(1).nodetoolResult("repair", KEYSPACE, table);
            result.asserts().success();

            // Wait for SSTables to be marked repaired
            cluster.forEach(node -> node.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                for (int attempt = 0; attempt < 100; attempt++)
                {
                    if (cfs.getLiveSSTables().stream().allMatch(SSTableReader::isRepaired))
                        return;
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                }
                Assert.fail("SSTables were not all marked repaired after IR");
            }));

            // Insert more data (creates unrepaired SSTables alongside repaired ones)
            for (int i = 150; i < 200; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + table + " (k, v) VALUES (?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            cluster.forEach(node -> node.flush(KEYSPACE));

            // Verify mixed repaired/unrepaired state on at least one node
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                boolean hasRepaired = cfs.getLiveSSTables().stream().anyMatch(SSTableReader::isRepaired);
                boolean hasUnrepaired = cfs.getLiveSSTables().stream().anyMatch(s -> !s.isRepaired());
                Assert.assertTrue("Should have repaired SSTables", hasRepaired);
                Assert.assertTrue("Should have unrepaired SSTables after new writes", hasUnrepaired);
            });

            // Trigger compaction - this should work without mixing repaired and unrepaired
            cluster.forEach(node -> node.forceCompact(KEYSPACE, table));

            // Verify data integrity
            Object[][] rows = cluster.coordinator(1).execute("SELECT count(*) FROM " + KEYSPACE + "." + table,
                                                              ConsistencyLevel.ALL);
            Assert.assertEquals(200L, rows[0][0]);
        }
    }

    @Test
    public void testSecondIncrementalRepairAfterNewWrites() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .start()))
        {
            String table = "tbl3";
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + "." + table + " (k int PRIMARY KEY, v int)" +
                                 " WITH compaction = {'class': '" + ROCS + "'}");

            // First batch of data
            for (int i = 0; i < 50; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + table + " (k, v) VALUES (?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            cluster.forEach(node -> node.flush(KEYSPACE));

            // First IR
            cluster.get(1).nodetoolResult("repair", KEYSPACE, table).asserts().success();

            // Wait for first repair to complete
            waitAllRepaired(cluster, table);

            // Second batch of data (creates new unrepaired SSTables)
            for (int i = 50; i < 100; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + "." + table + " (k, v) VALUES (?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            cluster.forEach(node -> node.flush(KEYSPACE));

            // Second IR - should only repair the new unrepaired SSTables
            cluster.get(1).nodetoolResult("repair", KEYSPACE, table).asserts().success();

            // Wait for all to be repaired
            waitAllRepaired(cluster, table);

            // Verify all data
            Object[][] rows = cluster.coordinator(1).execute("SELECT count(*) FROM " + KEYSPACE + "." + table,
                                                              ConsistencyLevel.ALL);
            Assert.assertEquals(100L, rows[0][0]);
        }
    }

    private static void waitAllRepaired(Cluster cluster, String table)
    {
        cluster.forEach(node -> node.runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
            for (int attempt = 0; attempt < 100; attempt++)
            {
                if (cfs.getLiveSSTables().stream().allMatch(SSTableReader::isRepaired))
                    return;
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
            Assert.fail("SSTables were not all marked repaired within timeout");
        }));
    }
}
