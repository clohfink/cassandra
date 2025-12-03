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
package org.apache.cassandra.db.compaction.writers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.fail;

/**
 * Tests for MaxSSTableSizeWriter, specifically targeting the regression
 * causes CPU spinning during compaction due to excessive SSTable switching.
 */
public class MaxSSTableSizeWriterTest extends CQLTester
{
    /**
     * Tests the regression: LCS L0 compaction with token boundaries causes infinite loop.
     *
     * The bug in production logs:
     * - Compact 2 L0 SSTables -> creates 2 new L0 SSTables (split on token boundary)
     * - LCS sees 2 L0 SSTables -> immediately compacts them again
     * - Creates 2 new L0 SSTables (split on token boundary again)
     * - Infinite loop! CPU spin!
     *
     * Example from logs:
     * Compacted 2 sstables to [nb-9203754-big, nb-9203755-big] to level=0
     * Compacting [nb-9203754-big, nb-9203755-big]
     * Compacted 2 sstables to [nb-9203757-big, nb-9203758-big] to level=0
     * Compacting [nb-9203757-big, nb-9203758-big]
     * ... repeats forever
     *
     * Root cause: MaxSSTableSizeWriter splits on token boundaries even for L0 compactions.
     * When 2 L0 SSTables span a token boundary, compaction creates 2 L0 SSTables back,
     * and LCS triggers another compaction immediately.
     */
    @Test
    public void testLCSL0CompactionDoesNotInfiniteLoopOnTokenBoundaries() throws Throwable
    {
        createTable("CREATE TABLE %s (k blob, v blob, PRIMARY KEY (k))");

        // Set up a token boundary that will split our data
        List<Token> tokens = Arrays.asList(
            new LongToken(0L)  // Single boundary in the middle
        );
        StorageService.instance.getTokenMetadata().updateNormalTokens(tokens, FBUtilities.getBroadcastAddressAndPort());

        // Use LCS
        alterTable("ALTER TABLE %s WITH compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '1'}");

        // Create >32 L0 SSTables to trigger STCS in L0 (MAX_COMPACTING_L0 = 32)
        // Each flush creates 1 L0 SSTable, and we need them to span the token boundary
        byte[] smallValue = new byte[100];
        ByteBuffer smallValueBuffer = ByteBuffer.wrap(smallValue);

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // Create 35 L0 SSTables, each spanning the token boundary at 0
        // This triggers getSTCSInL0CompactionCandidate which uses LeveledCompactionTask with level=0
        for (int flush = 0; flush < 35; flush++)
        {
            // Each flush writes partitions on both sides of token boundary 0
            for (int i = 0; i < 5; i++)
            {
                // Alternate negative and positive tokens to span boundary
                long tokenValue = (flush % 2 == 0)
                    ? -5000000000000000000L + (i * 100000000000000L)
                    : 1000000000000000L + (i * 100000000000000L);
                ByteBuffer key = LongToken.keyForToken(new LongToken(tokenValue));
                String hexKey = ByteBufferUtil.bytesToHex(key);
                execute("INSERT INTO %s (k, v) VALUES (0x" + hexKey + ", ?)", smallValueBuffer);
            }
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        // THE BUG: With token boundaries, each STCS in L0 compaction will split outputs
        // This means compactions keep finishing but never reduce the L0 count below the threshold
        // We'll monitor: compactions keep completing, but L0 SSTable count stays high

        long compactionsCompleted = CompactionManager.instance.getCompletedTasks();
        long startTime = System.currentTimeMillis();
        boolean cleared = false;
        while (System.currentTimeMillis() - startTime < 15000)
        {
            Thread.sleep(1000);
            if (compactionsCompleted == CompactionManager.instance.getCompletedTasks())
            {
                cleared = true;
                break;
            }
            compactionsCompleted = CompactionManager.instance.getCompletedTasks();
        }
        if (!cleared)
        {
            fail("Compactions never settle");
        }

    }

}