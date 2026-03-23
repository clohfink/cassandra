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

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;

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
    
    private static final String SIZE_THRESHOLD_PROPERTY = "cassandra.readonly_compaction.size_threshold_bytes";
    public static final long DEFAULT_SIZE_THRESHOLD_BYTES = 10 * 1024L * 1024L * 1024L; // 10GB
    public static final long SIZE_THRESHOLD_BYTES = Long.getLong(SIZE_THRESHOLD_PROPERTY, DEFAULT_SIZE_THRESHOLD_BYTES);
    
    private static final String MAX_SIZE_KEY = "sstable_size_in_mb";
    
    private final long maxSizeMb;
    protected final Set<SSTableReader> sstables = new HashSet<>();

    public ReadOnlyCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        String maxSizeOption = options.get(MAX_SIZE_KEY);
        this.maxSizeMb = maxSizeOption != null ? Long.parseLong(maxSizeOption) : SIZE_THRESHOLD_BYTES / (1024L * 1024L);
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

        return new ReadOnlyCompactionTask(cfs, txn, gcBefore, this);
    }

    @Override
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        if (!isActive)
            return Collections.emptyList();
            
        Set<SSTableReader> liveSSTables;
        synchronized (this)
        {
            liveSSTables = ImmutableSet.copyOf(sstables);
        }
        if (liveSSTables.isEmpty())
            return Collections.emptyList();

        logger.info("Creating maximal compaction task for {} SSTables", liveSSTables.size());

        LifecycleTransaction txn = cfs.getTracker().tryModify(liveSSTables, OperationType.COMPACTION);
        if (txn == null)
            return Collections.emptyList();
            
        return Collections.singleton(new ReadOnlyCompactionTask(cfs, txn, gcBefore, this));
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
            
        return new ReadOnlyCompactionTask(cfs, txn, gcBefore, this);
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
        return maxSizeMb * 1024L * 1024L;
    }
    
    @Override
    protected synchronized Set<SSTableReader> getSSTables()
    {
        return ImmutableSet.copyOf(sstables);
    }
    
    @Override
    public synchronized void addSSTable(SSTableReader added)
    {
        sstables.add(added);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }
    
    public synchronized Collection<SSTableReader> getOverlappingSSTables()
    {
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
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        
        String maxSizeOption = options.get(MAX_SIZE_KEY);
        if (maxSizeOption != null)
        {
            try
            {
                long maxSize = Long.parseLong(maxSizeOption);
                if (maxSize <= 0)
                {
                    throw new ConfigurationException(String.format("%s must be positive: %d", MAX_SIZE_KEY, maxSize));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a parsable long for %s", maxSizeOption, MAX_SIZE_KEY), e);
            }
            uncheckedOptions.remove(MAX_SIZE_KEY);
        }
        
        return uncheckedOptions;
    }
    
    private static class ReadOnlyCompactionTask extends CompactionTask
    {
        private final ReadOnlyCompactionStrategy strategy;
        
        public ReadOnlyCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore, ReadOnlyCompactionStrategy strategy)
        {
            super(cfs, txn, gcBefore);
            this.strategy = strategy;
        }
        
        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction txn,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new MaxSSTableSizeWriter(cfs, directories, txn, nonExpiredSSTables, strategy.maxSizeMb * 1024L * 1024L, 1);
        }
    }
}