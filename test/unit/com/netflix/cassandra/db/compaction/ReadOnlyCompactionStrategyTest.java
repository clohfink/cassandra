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
package com.netflix.cassandra.db.compaction;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadOnlyCompactionStrategyTest extends CQLTester
{
    @Test
    public void testTokenMapSplitsSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (k blob, v int, PRIMARY KEY (k))");
        
        // Set up token metadata with multiple nodes to create token boundaries
        // This will cause the ReadOnlyCompactionStrategy to split SSTables at token boundaries
        List<Token> tokens = Arrays.asList(
            new LongToken(-6000000000000000000L),
            new LongToken(-2000000000000000000L),
            new LongToken(2000000000000000000L),
            new LongToken(6000000000000000000L)
        );
        StorageService.instance.getTokenMetadata().updateNormalTokens(tokens, FBUtilities.getBroadcastAddressAndPort());
        
        // Change to ReadOnlyCompactionStrategy before inserting data
        alterTable("ALTER TABLE %s WITH compaction = {'class': 'com.netflix.cassandra.db.compaction.ReadOnlyCompactionStrategy'}");
        
        // Insert data across multiple token ranges to create overlapping SSTables
        long[] testTokens = {1L, 2L, 3L, 2000000000000000001L, 2000000000000000002L};
        
        for (int i = 0; i < testTokens.length; i++)
        {
            ByteBuffer key = LongToken.keyForToken(new LongToken(testTokens[i]));
            String hexKey = ByteBufferUtil.bytesToHex(key);
            execute("INSERT INTO %s (k, v) VALUES (0x" + hexKey + ", ?)", i);
            
            // Flush after each insert to create separate SSTables
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }
        
        // Get the column family store and wait for any automatic compactions to complete
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        while (CompactionManager.instance.isCompacting(Collections.singletonList(cfs), sstable -> true))
        {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        
        int sstableCountBefore = cfs.getLiveSSTables().size();
        assertTrue("Should have multiple SSTables before maximal compaction", sstableCountBefore >= 2);
        
        // Force a maximal compaction to trigger the token-based splitting
        CompactionManager.instance.submitMaximal(cfs, cfs.gcBefore((int)(System.currentTimeMillis() / 1000)), false);
        Thread.sleep(100);
        
        // Wait for maximal compaction to complete
        while (CompactionManager.instance.isCompacting(Collections.singletonList(cfs), sstable -> true))
        {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        
        // Verify SSTables were created (should be split based on token ranges)
        int sstableCountAfter = cfs.getLiveSSTables().size();
        
        // The exact count depends on how the data spans token ranges, but should be > 1 due to token boundaries
        assertTrue("Should have at least 1 SSTable after compaction", sstableCountAfter >= 1);
        assertEquals("Should have exactly 2 SSTables after maximal compaction", 2, sstableCountAfter);
        
        // Verify all data is still accessible
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(5L));
        
        // Verify individual rows are still accessible
        for (int i = 0; i < testTokens.length; i++)
        {
            ByteBuffer key = LongToken.keyForToken(new LongToken(testTokens[i]));
            String hexKey = ByteBufferUtil.bytesToHex(key);
            assertRows(execute("SELECT v FROM %s WHERE k = 0x" + hexKey), row(i));
        }
    }
    
    @Test
    public void testBackgroundCompactionMinimalSSTables() throws Throwable
    {
        createTable("CREATE TABLE %s (k blob, v int, PRIMARY KEY (k)) WITH compaction = {'class': 'com.netflix.cassandra.db.compaction.ReadOnlyCompactionStrategy'}");
        
        // Set up token metadata with multiple nodes to create token boundaries
        List<Token> tokens = Arrays.asList(
            new LongToken(-6000000000000000000L),
            new LongToken(-2000000000000000000L),
            new LongToken(2000000000000000000L),
            new LongToken(6000000000000000000L)
        );
        StorageService.instance.getTokenMetadata().updateNormalTokens(tokens, FBUtilities.getBroadcastAddressAndPort());

        // Insert data across multiple token ranges, creating multiple overlapping SSTables
        // First token range: < -6000000000000000000L
        long[] firstRangeTokens = {-8000000000000000000L, -7000000000000000000L};
        // Second token range: -6000000000000000000L to -2000000000000000000L  
        long[] secondRangeTokens = {-4000000000000000000L, -3000000000000000000L};
        // Third token range: -2000000000000000000L to 2000000000000000000L
        long[] thirdRangeTokens = {0L, 1000000000000000000L};
        // Fourth token range: 2000000000000000000L to 6000000000000000000L
        long[] fourthRangeTokens = {4000000000000000000L, 5000000000000000000L};
        
        // Create overlapping SSTables by inserting data from different ranges
        long[][] allRanges = {firstRangeTokens, secondRangeTokens, thirdRangeTokens, fourthRangeTokens};
        int valueCounter = 0;
        
        for (int round = 0; round < 3; round++) // Create 3 rounds of overlapping SSTables
        {
            for (long[] range : allRanges)
            {
                for (int tokenIndex = 0; tokenIndex < range.length; tokenIndex++)
                {
                    // Create unique token values by adding round and index offsets
                    long uniqueTokenValue = range[tokenIndex] + (round * 1000) + tokenIndex;
                    ByteBuffer key = LongToken.keyForToken(new LongToken(uniqueTokenValue));
                    String hexKey = ByteBufferUtil.bytesToHex(key);
                    execute("INSERT INTO %s (k, v) VALUES (0x" + hexKey + ", ?)", valueCounter++);
                }
                // Flush after each range to create overlapping SSTables
                ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
                cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            }
        }
        
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        
        // Wait for any ongoing compactions to complete before checking
        while (CompactionManager.instance.isCompacting(Collections.singletonList(cfs), sstable -> true))
        {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        
        int sstableCountBefore = cfs.getLiveSSTables().size();
        
        // We should have some overlapping SSTables before manual compaction
        assertTrue("Should have multiple overlapping SSTables before compaction", sstableCountBefore >= 4);
        
        // Trigger background compaction (non-maximal)
        CompactionManager.instance.submitBackground(cfs);
        Thread.sleep(100);
        
        // Wait for compaction to complete
        while (CompactionManager.instance.isCompacting(Collections.singletonList(cfs), sstable -> true))
        {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        
        // Verify that compaction has reduced to minimal SSTables per token range
        int sstableCountAfter = cfs.getLiveSSTables().size();
        
        // Should have exactly 4 SSTables - one per token range (minimal possible)
        assertEquals("Should compact to minimal 4 SSTables (one per token range)", 4, sstableCountAfter);
        
        // Verify all data is still accessible
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(24L)); // 3 rounds * 4 ranges * 2 tokens each
        
        // Verify data integrity for each token range using the same unique key generation
        int expectedValue = 0;
        for (int round = 0; round < 3; round++)
        {
            for (long[] range : allRanges)
            {
                for (int tokenIndex = 0; tokenIndex < range.length; tokenIndex++)
                {
                    long uniqueTokenValue = range[tokenIndex] + (round * 1000) + tokenIndex;
                    ByteBuffer key = LongToken.keyForToken(new LongToken(uniqueTokenValue));
                    String hexKey = ByteBufferUtil.bytesToHex(key);
                    assertRows(execute("SELECT v FROM %s WHERE k = 0x" + hexKey), row(expectedValue));
                    expectedValue++;
                }
            }
        }
    }
    
    @Test
    public void testOverlapDetectionMissedCase() throws Throwable
    {
        createTable("CREATE TABLE %s (k blob, v int, PRIMARY KEY (k)) WITH compaction = {'class': 'com.netflix.cassandra.db.compaction.ReadOnlyCompactionStrategy', 'enabled': 'false'}");
        
        // Test the specific case mentioned: [t1, e2], [t2, e1], [t3, e3] 
        // where t1 < t2 < t3 and e1 < e3 < e2
        // This should detect overlap between [t1, e2] and [t3, e3]
        
        long t1 = 1000L;
        long t2 = 2000L; 
        long t3 = 3000L;
        long e1 = 1500L;  // e1 < e3 < e2
        long e2 = 4000L;
        long e3 = 3500L;
        
        // Create SSTables with these token ranges
        createSSTableWithTokenRange(t1, e2, 1);  // [1000, 4000]
        createSSTableWithTokenRange(t2, e1, 2);  // [2000, 1500] - wrapped range
        createSSTableWithTokenRange(t3, e3, 3);  // [3000, 3500]
        
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ReadOnlyCompactionStrategy strategy = (ReadOnlyCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(0).get(0);
        
        // Should detect overlaps: [t1,e2] overlaps [t3,e3] since t3=3000 < e2=4000
        Collection<SSTableReader> overlapping = strategy.getOverlappingSSTables();
        
        // All three SSTables should be considered overlapping in this complex case
        assertTrue("Should detect overlapping SSTables in the missed case scenario", overlapping.size() >= 2);
        
        // Verify data integrity - we insert 2 rows per SSTable (start and end token)
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(6L));
    }
    
    @Test
    public void testOverlapDetectionEdgeCases() throws Throwable
    {
        createTable("CREATE TABLE %s (k blob, v int, PRIMARY KEY (k)) WITH compaction = {'class': 'com.netflix.cassandra.db.compaction.ReadOnlyCompactionStrategy', 'enabled': 'false'}");
        
        // Test Case 1: Adjacent non-overlapping ranges
        createSSTableWithTokenRange(1000L, 2000L, 1);  // [1000, 2000]
        createSSTableWithTokenRange(2001L, 3000L, 2);  // [2001, 3000] - no overlap
        
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ReadOnlyCompactionStrategy strategy = (ReadOnlyCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(0).get(0);
        
        Collection<SSTableReader> overlapping = strategy.getOverlappingSSTables();
        assertEquals("Adjacent non-overlapping ranges should not be detected as overlapping", 0, overlapping.size());
        
        // Test Case 2: Exactly touching ranges (boundary case)
        clearTable();
        createSSTableWithTokenRange(1000L, 2000L, 3);  // [1000, 2000]
        createSSTableWithTokenRange(2000L, 3000L, 4);  // [2000, 3000] - touching at 2000
        
        overlapping = strategy.getOverlappingSSTables();
        assertTrue("Touching ranges should be detected as overlapping", overlapping.size() >= 2);
        
        // Test Case 3: Complete containment
        clearTable();
        createSSTableWithTokenRange(1000L, 5000L, 5);  // [1000, 5000] - outer range
        createSSTableWithTokenRange(2000L, 3000L, 6);  // [2000, 3000] - completely contained
        
        overlapping = strategy.getOverlappingSSTables();
        assertEquals("Completely contained ranges should be detected as overlapping", 2, overlapping.size());
        
        // Test Case 4: Partial overlap (standard case)
        clearTable();
        createSSTableWithTokenRange(1000L, 3000L, 7);  // [1000, 3000]
        createSSTableWithTokenRange(2000L, 4000L, 8);  // [2000, 4000] - overlaps 2000-3000
        
        overlapping = strategy.getOverlappingSSTables();
        assertEquals("Partially overlapping ranges should be detected", 2, overlapping.size());
    }
    
    @Test
    public void testOverlapDetectionMultipleComplexRanges() throws Throwable
    {
        createTable("CREATE TABLE %s (k blob, v int, PRIMARY KEY (k)) WITH compaction = {'class': 'com.netflix.cassandra.db.compaction.ReadOnlyCompactionStrategy', 'enabled': 'false'}");
        
        // Test complex scenario with multiple overlapping patterns
        // Range A: [1000, 8000] - spans across multiple others
        // Range B: [2000, 3000] - contained in A
        // Range C: [3500, 4500] - overlaps A, separate from B  
        // Range D: [7000, 9000] - overlaps A
        // Range E: [10000, 11000] - separate from all others
        
        createSSTableWithTokenRange(1000L, 8000L, 1);   // A
        createSSTableWithTokenRange(2000L, 3000L, 2);   // B  
        createSSTableWithTokenRange(3500L, 4500L, 3);   // C
        createSSTableWithTokenRange(7000L, 9000L, 4);   // D
        createSSTableWithTokenRange(10000L, 11000L, 5); // E
        
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        
        // Wait for any ongoing compactions to complete
        while (CompactionManager.instance.isCompacting(Collections.singletonList(cfs), sstable -> true))
        {
            try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        
        ReadOnlyCompactionStrategy strategy = (ReadOnlyCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(0).get(0);
        Collection<SSTableReader> overlapping = strategy.getOverlappingSSTables();
        
        // A, B, C, D should all be detected as overlapping (4 SSTables)
        // E should not be included (separate range)
        assertEquals("Should detect 4 overlapping SSTables in complex scenario", 4, overlapping.size());
        
        // Verify E is not included by checking that we can identify the separate SSTable
        Set<SSTableReader> allSSTables = new HashSet<>(cfs.getLiveSSTables());
        Set<SSTableReader> overlappingSet = new HashSet<>(overlapping);
        allSSTables.removeAll(overlappingSet);
        assertEquals("Should have exactly 1 non-overlapping SSTable (E)", 1, allSSTables.size());
        
        assertRows(execute("SELECT COUNT(*) FROM %s"), row(10L));
    }
    
    @Test 
    public void testNoOverlapWithSingleSSTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k blob, v int, PRIMARY KEY (k)) WITH compaction = {'class': 'com.netflix.cassandra.db.compaction.ReadOnlyCompactionStrategy', 'enabled': 'false'}");
        
        // Single SSTable should never be considered overlapping
        createSSTableWithTokenRange(1000L, 2000L, 1);
        
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        ReadOnlyCompactionStrategy strategy = (ReadOnlyCompactionStrategy) cfs.getCompactionStrategyManager().getStrategies().get(0).get(0);
        
        Collection<SSTableReader> overlapping = strategy.getOverlappingSSTables();
        assertEquals("Single SSTable should not be detected as overlapping", 0, overlapping.size());
    }
    
    private void createSSTableWithTokenRange(long startToken, long endToken, int value) throws Throwable
    {
        // Insert data at start and end of token range to define the SSTable bounds
        ByteBuffer startKey = LongToken.keyForToken(new LongToken(startToken));
        ByteBuffer endKey = LongToken.keyForToken(new LongToken(endToken));
        
        String startHex = ByteBufferUtil.bytesToHex(startKey);
        String endHex = ByteBufferUtil.bytesToHex(endKey);
        
        execute("INSERT INTO %s (k, v) VALUES (0x" + startHex + ", ?)", value);
        execute("INSERT INTO %s (k, v) VALUES (0x" + endHex + ", ?)", value);
        
        // Flush to create the SSTable
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }
    
    private void clearTable() throws Throwable
    {
        execute("TRUNCATE %s");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.truncateBlocking();
    }
}