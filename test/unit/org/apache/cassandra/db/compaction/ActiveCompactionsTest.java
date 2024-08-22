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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.cache.AutoSavingCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.db.view.ViewBuilderTask;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.io.sstable.IndexSummaryRedistribution;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ActiveCompactionsTest extends CQLTester
{
    @Test
    public void testIndexSummaryRedistributionTracking() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)");
            flush();
        }
        Set<SSTableReader> sstables = getCurrentColumnFamilyStore().getLiveSSTables();
        try (LifecycleTransaction txn = getCurrentColumnFamilyStore().getTracker().tryModify(sstables, OperationType.INDEX_SUMMARY))
        {
            Map<TableId, LifecycleTransaction> transactions = ImmutableMap.<TableId, LifecycleTransaction>builder().put(getCurrentColumnFamilyStore().metadata().id, txn).build();
            IndexSummaryRedistribution isr = new IndexSummaryRedistribution(transactions, 0, 1000);
            MockActiveCompactions mockActiveCompactions = new MockActiveCompactions();
            CompactionManager.instance.runIndexSummaryRedistribution(isr, mockActiveCompactions);
            assertTrue(mockActiveCompactions.finished);
            assertNotNull(mockActiveCompactions.holder);
            // index redistribution operates over all keyspaces/tables, we always cancel them
            assertTrue(mockActiveCompactions.holder.getCompactionInfo().getSSTables().isEmpty());
            assertTrue(mockActiveCompactions.holder.getCompactionInfo().shouldStop((sstable) -> false));
        }
    }

    @Test
    public void testScrubOne() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)");
            flush();
        }

        SSTableReader sstable = Iterables.getFirst(getCurrentColumnFamilyStore().getLiveSSTables(), null);
        try (LifecycleTransaction txn = getCurrentColumnFamilyStore().getTracker().tryModify(sstable, OperationType.SCRUB))
        {
            MockActiveCompactions mockActiveCompactions = new MockActiveCompactions();
            CompactionManager.instance.scrubOne(getCurrentColumnFamilyStore(), txn, true, false, false, mockActiveCompactions);

            assertTrue(mockActiveCompactions.finished);
            assertEquals(mockActiveCompactions.holder.getCompactionInfo().getSSTables(), Sets.newHashSet(sstable));
            assertFalse(mockActiveCompactions.holder.getCompactionInfo().shouldStop((s) -> false));
            assertTrue(mockActiveCompactions.holder.getCompactionInfo().shouldStop((s) -> true));
        }
    }

    @Test
    public void testVerifyOne() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY (pk, ck))");
        getCurrentColumnFamilyStore().disableAutoCompaction();
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (pk, ck, a, b) VALUES (" + i + ", 2, 3, 4)");
            flush();
        }

        SSTableReader sstable = Iterables.getFirst(getCurrentColumnFamilyStore().getLiveSSTables(), null);
        MockActiveCompactions mockActiveCompactions = new MockActiveCompactions();
        CompactionManager.instance.verifyOne(getCurrentColumnFamilyStore(), sstable, new Verifier.Options.Builder().build(), mockActiveCompactions);
        assertTrue(mockActiveCompactions.finished);
        assertEquals(mockActiveCompactions.holder.getCompactionInfo().getSSTables(), Sets.newHashSet(sstable));
        assertFalse(mockActiveCompactions.holder.getCompactionInfo().shouldStop((s) -> false));
        assertTrue(mockActiveCompactions.holder.getCompactionInfo().shouldStop((s) -> true));
    }

    @Test
    public void testSubmitCacheWrite() throws ExecutionException, InterruptedException
    {
        CacheService.instance.keyCache.setCapacity(32);
        AutoSavingCache.Writer writer = CacheService.instance.keyCache.getWriter(100);
        MockActiveCompactions mockActiveCompactions = new MockActiveCompactions();
        CompactionManager.instance.submitCacheWrite(writer, mockActiveCompactions).get();
        assertTrue(mockActiveCompactions.finished);
        assertTrue(mockActiveCompactions.holder.getCompactionInfo().getSSTables().isEmpty());
    }

    private static class MockActiveCompactions implements ActiveCompactionsTracker
    {
        public CompactionInfo.Holder holder;
        public boolean finished = false;
        public void beginCompaction(CompactionInfo.Holder ci)
        {
            holder = ci;
        }

        public void finishCompaction(CompactionInfo.Holder ci)
        {
            finished = true;
        }
    }
}
