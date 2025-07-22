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

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.DefaultCompactionWriter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;

/**
 * Netflix-specific compaction strategy designed for read-only tables.
 * 
 * This strategy aims to compact all SSTables into non-overlapping files while
 * respecting token boundaries for optimal read performance. It's designed for
 * tables that don't receive writes after initial data load.
 * 
 * Key features:
 * - Compacts all overlapping SSTables into non-overlapping ones
 * - Splits output at token boundaries to respect cluster topology
 *    - Lets bootstrapping and repairs use fast streaming
 * - Optimized for read-heavy workloads
 * - Minimal background compaction (only when new data is added)
 */
public class ReadOnlyCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyCompactionStrategy.class);
    
    
    public ReadOnlyCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        logger.debug("ReadOnlyCompactionStrategy initialized for {}.{}",
                    cfs.keyspace.getName(), cfs.name);
    }

    @Override
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        if (!isActive)
            return null;
            
        Collection<SSTableReader> sstables = getOverlappingSSTables();
        if (sstables.size() < 2)
            return null;
            
        logger.debug("Found {} overlapping SSTables for background compaction", sstables.size());
        
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (txn == null)
            return null;
            
        return new ReadOnlyCompactionTask(cfs, txn, gcBefore);
    }

    @Override
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        if (!isActive)
            return Collections.emptyList();
            
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        if (sstables.isEmpty())
            return Collections.emptyList();
            
        logger.info("Creating maximal compaction task for {} SSTables", sstables.size());
        
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (txn == null)
            return Collections.emptyList();
            
        return Collections.singleton(new ReadOnlyCompactionTask(cfs, txn, gcBefore));
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        if (!isActive || sstables.isEmpty())
            return null;
            
        logger.info("Creating user-defined compaction task for {} SSTables", sstables.size());
        
        LifecycleTransaction txn = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (txn == null)
            return null;
            
        return new ReadOnlyCompactionTask(cfs, txn, gcBefore);
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        Collection<SSTableReader> overlapping = getOverlappingSSTables();
        return overlapping.size() > 1 ? 1 : 0;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE; // Allow large SSTables for read-only workloads
    }
    
    @Override
    protected Set<SSTableReader> getSSTables()
    {
        return cfs.getLiveSSTables();
    }
    
    @Override
    public void addSSTable(SSTableReader added)
    {
        // For read-only strategy, we don't need to do anything special when SSTable is added
        // The strategy will handle it in the next compaction cycle if needed
        logger.debug("Added SSTable {} to ReadOnlyCompactionStrategy", added);
    }
    
    @Override
    public void removeSSTable(SSTableReader sstable)
    {
        // For read-only strategy, we don't maintain internal state for SSTables
        // so there's nothing special to do when removing
        logger.debug("Removed SSTable {} from ReadOnlyCompactionStrategy", sstable);
    }
    
    public Collection<SSTableReader> getOverlappingSSTables()
    {
        Collection<SSTableReader> sstables = cfs.getLiveSSTables();
        if (sstables.size() < 2)
            return Collections.emptyList();

        // Use sweep line algorithm
        List<SSTableReader> sortedByStart = new ArrayList<>(sstables);
        List<SSTableReader> sortedByEnd = new ArrayList<>(sstables);
        
        // Sort by start and end tokens
        sortedByStart.sort((a, b) -> a.first.getToken().compareTo(b.first.getToken()));
        sortedByEnd.sort((a, b) -> a.last.getToken().compareTo(b.last.getToken()));
        
        Set<SSTableReader> overlapping = new HashSet<>();
        Set<SSTableReader> activeIntervals = new HashSet<>();
        
        int startIdx = 0, endIdx = 0;
        
        // Sweep through start and end events using two pointers
        while (startIdx < sortedByStart.size() || endIdx < sortedByEnd.size())
        {
            boolean processStart = false;
            
            if (startIdx >= sortedByStart.size())
            {
                // Only end events left
                processStart = false;
            }
            else if (endIdx >= sortedByEnd.size())
            {
                // Only start events left
                processStart = true;
            }
            else
            {
                // Compare next start vs next end token
                Token nextStart = sortedByStart.get(startIdx).first.getToken();
                Token nextEnd = sortedByEnd.get(endIdx).last.getToken();
                int compare = nextStart.compareTo(nextEnd);
                
                // Process start events before end events at same token
                processStart = (compare <= 0);
            }
            
            if (processStart)
            {
                SSTableReader sstable = sortedByStart.get(startIdx++);
                // If there are already active intervals, we have overlaps
                if (!activeIntervals.isEmpty())
                {
                    overlapping.add(sstable);
                    overlapping.addAll(activeIntervals);
                }
                activeIntervals.add(sstable);
            }
            else
            {
                SSTableReader sstable = sortedByEnd.get(endIdx++);
                activeIntervals.remove(sstable);
            }
        }
        
        return overlapping;
    }
    
    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return AbstractCompactionStrategy.validateOptions(options);
    }
    
    private static class ReadOnlyCompactionTask extends CompactionTask
    {
        public ReadOnlyCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore)
        {
            super(cfs, txn, gcBefore);
        }
        
        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction txn,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new ReadOnlyCompactionWriter(cfs, directories, txn, nonExpiredSSTables);
        }
    }
    
    private static class ReadOnlyCompactionWriter extends DefaultCompactionWriter
    {
        private int lastTokenRangeIndex = Integer.MIN_VALUE;
        private Directories.DataDirectory currentDirectory;
        private final List<Token> sortedTokens;
        private final Token[] tokenArray;
        
        public ReadOnlyCompactionWriter(ColumnFamilyStore cfs,
                                       Directories directories,
                                       LifecycleTransaction txn,
                                       Set<SSTableReader> nonExpiredSSTables)
        {
            super(cfs, directories, txn, nonExpiredSSTables);
            this.currentDirectory = getDirectories().getWriteableLocation(getExpectedWriteSize());
            this.sortedTokens = StorageService.instance.getTokenMetadata().sortedTokens();
            this.tokenArray = sortedTokens.toArray(new Token[0]);
        }
        
        @Override
        public boolean realAppend(UnfilteredRowIterator partition)
        {
            try {
                Token partitionToken = partition.partitionKey().getToken();
                int tokenRangeIndex = Arrays.binarySearch(tokenArray, partitionToken);
                
                // binarySearch returns negative value if not found, convert to insertion point
                if (tokenRangeIndex < 0) {
                    tokenRangeIndex = -(tokenRangeIndex + 1);
                }
                
                // If the partition has moved to a different token range, switch to a new SSTable
                if (tokenRangeIndex != lastTokenRangeIndex && lastTokenRangeIndex != Integer.MIN_VALUE) {
                    switchCompactionLocation(currentDirectory);
                }
                
                lastTokenRangeIndex = tokenRangeIndex;
                return super.realAppend(partition);
            } catch (Exception e) {
                logger.error("Error during partition append in ReadOnlyCompactionWriter", e);
                throw e;
            }
        }
        
        @Override
        public void switchCompactionLocation(Directories.DataDirectory directory)
        {
            this.currentDirectory = directory;
            super.switchCompactionLocation(directory);
        }
    }
}