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

package com.netflix.cassandra.backups;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.commitlog.CommitLogPosition.NONE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class BackupMemtable implements Memtable
{
    private static final Logger logger = LoggerFactory.getLogger(BackupMemtable.class);

    private final TableMetadataRef metadataRef;
    private final BackupMemtableParams params;
    private final BackupMemtableContext context;
    private final Future<?> initializationFuture;

    public BackupMemtable(TableMetadataRef metadataRef,
                          BackupMemtableParams params)
    {
        this.metadataRef = metadataRef;
        this.params = params;
        this.context = new BackupMemtableContext(params, metadataRef);
        this.initializationFuture = Stage.NETFLIX.submit(context);
    }

    /**
     * @return the list of BackupDescriptors for SSTables that have FILTER, SUMMARY, STATS and COMPRESSION_INFO
     *         available locally (either pre-existing or successfully downloaded).
     */
    public List<BackupDescriptor> getDescriptors()
    {
        return context.getDescriptors();
    }

    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        // hints or RRs might get here. throwing exception can break things, so we just log it
        NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, TimeUnit.MINUTES,
                         "BackupMemtable update ignored: {}", update);
        return 0;
    }

    @Override
    public long partitionCount()
    {
        return SSTableReader.getApproximateKeyCount(context.getSstables());
    }


    @Override
    public long getLiveDataSize()
    {
        // TODO can probably use this to expose components that have been downloaded size in metrics
        return 0;
    }

    @Override
    public long operationCount()
    {
        return 0;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadataRef.get();
    }

    @Override
    public void addMemoryUsageTo(MemoryUsage usage)
    {
        // Not added because
        //  - BackupMemtable is read-only, doesn't allocate like a normal memtable
        //  - Won't be selected for flushing (correct, since it can't flush anyway)
    }

    @Override
    public void markExtraOnHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {

    }

    @Override
    public void markExtraOffHeapUsed(long additionalSpace, OpOrder.Group opGroup)
    {

    }

    @Override
    public FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        return new FlushablePartitionSet<Partition>()
        {
            @Override
            public Memtable memtable()
            {
                return BackupMemtable.this;
            }

            @Override
            public PartitionPosition from()
            {
                return from;
            }

            @Override
            public PartitionPosition to()
            {
                return to;
            }

            @Override
            public CommitLogPosition commitLogLowerBound()
            {
                return NONE;
            }

            @Override
            public CommitLogPosition commitLogUpperBound()
            {
                return NONE;
            }

            @Override
            public RegularAndStaticColumns columns()
            {
                return RegularAndStaticColumns.NONE;
            }

            @Override
            public EncodingStats encodingStats()
            {
                return EncodingStats.NO_STATS;
            }

            @Override
            public long partitionCount()
            {
                return 0;
            }

            @Override
            public long partitionKeysSize()
            {
                return 0;
            }

            @Override
            public long dataSize()
            {
                return 0;
            }

            @Override
            public Iterator<Partition> iterator()
            {
                return Collections.emptyIterator();
            }
        };
    }

    @Override
    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {

    }

    @Override
    public void discard()
    {
        // Release all SSTableReader references to prevent ChannelProxy leaks
        for (SSTableReader sstable : context.getSstables())
        {
            sstable.selfRef().release();
        }
    }

    @Override
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        // if this returns false it will throw assertion error that there are no
        // memtables accepting writes, we want to throw from put() instead
        return true;
    }

    @Override
    public CommitLogPosition getApproximateCommitLogLowerBound()
    {
        return NONE;
    }

    @Override
    public CommitLogPosition getCommitLogLowerBound()
    {
        return NONE;
    }

    @Override
    public LastCommitLogPosition getFinalCommitLogUpperBound()
    {
        return new LastCommitLogPosition(NONE);
    }

    @Override
    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return false;
    }

    @Override
    public boolean isClean()
    {
        return false;
    }

    @Override
    public boolean shouldSwitch(ColumnFamilyStore.FlushReason reason)
    {
        return false;
    }

    @Override
    public void metadataUpdated()
    {

    }

    @Override
    public void localRangesUpdated()
    {
        // should rekick download
    }

    @Override
    public String toString()
    {
        return String.format("BackupMemtable{bucket='%s', prefix='%s', timestamp=%d}",
                             params.getBucket(), params.getPrefix(), params.getTimestamp());
    }

    @Override
    public void performSnapshot(String snapshotName)
    {
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key,
                                             Slices slices,
                                             ColumnFilter columnFilter,
                                             boolean reversed,
                                             SSTableReadsListener listener)
    {
        try
        {
            TableMetadata metadata = metadataRef.get();
            TableMetrics tableMetrics = ColumnFamilyStore.getIfExists(metadata.keyspace, metadata.name).metric;

            // Block on initialization before proceeding, return empty and log if it failed
            try
            {
                initializationFuture.get(
                    DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MILLISECONDS) / 2,
                    TimeUnit.MILLISECONDS
                );
            }
            catch (TimeoutException e)
            {
                throw new InvalidS3Exception("BackupMemtable initialization timed out for " + metadata.keyspace + "." + metadata.name, e);
            }
            catch (Exception e)
            {
                Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
                NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES,
                                 "BackupMemtable initialization failed, returning empty result for {}.{}: {}",
                                 metadata().keyspace, metadata().name, cause.getMessage());
                Tracing.trace("BackupMemtable initialization failed for {}.{}: {}", metadata.keyspace, metadata.name, cause.getMessage());
                return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
            }

            List<BackupDescriptor> descriptors = context.getDescriptors();

            // find all sstables covering this key
            List<SSTableReader> allSstables = context.getIntervalTree().search(key);
            ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, reversed);
            List<SSTableReader> filteredSstables = new ArrayList<>();
            for (SSTableReader st : allSstables)
            {
                if (filter.shouldInclude(st) && st.getBloomFilter().isPresent(key))
                {
                    filteredSstables.add(st);
                }
            }
            if (filteredSstables.isEmpty())
            {
                Tracing.trace("No sstables found for key {} in S3 backup {}", key, descriptors.size() > 0 ? descriptors.get(0) : "NONE");
                return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
            }

            // Create read context for tracking per-read metrics
            ReadContext readContext = new ReadContext();

            List<Future<UnfilteredRowIterator>> futures = new ArrayList<>();
            Tracing.trace("Found {} sstables of {} overlapping in S3 backup {}", filteredSstables.size(), allSstables.size(), descriptors.get(0));
            tableMetrics.updateSSTableIterated(filteredSstables.size());
            AtomicReference<Exception> exceptionRef = new AtomicReference<>();
            for (SSTableReader sstable : filteredSstables)
            {
                futures.add(Stage.NETFLIX.submit(() -> {
                    BackupChunkReader reader = null;
                    try
                    {
                        RowIndexEntry rie = sstable.getPosition(key, SSTableReader.Operator.EQ, listener);
                        if (rie == null) return null;
                        FileDataInput data = sstable.getFileDataInput(rie.position);
                        reader = (BackupChunkReader) data;

                        // Set read context on the reader instance
                        reader.setReadContext(readContext);

                        reader.prefetchPartition(rie);
                        UnfilteredRowIterator iter = sstable.rowIterator(data, key, rie, slices, columnFilter, reversed);
                        if (iter.hasNext())
                        {
                            // Wrap so closing the iterator also closes the BackupChunkReader,
                            // since rowIterator won't close a caller-provided FileDataInput.
                            BackupChunkReader readerToClose = reader;
                            return new WrappingUnfilteredRowIterator(iter)
                            {
                                public void close()
                                {
                                    try
                                    {
                                        super.close();
                                    }
                                    finally
                                    {
                                        readerToClose.close();
                                    }
                                }
                            };
                        }
                        // No data, close the reader
                        reader.close();
                        return null;
                    }
                    catch (Exception e)
                    {
                        exceptionRef.set(e);
                        if (reader != null)
                        {
                            reader.close();
                        }
                        return null;
                    }
                }));
            }

            // collect results, dropping any nulls
            List<UnfilteredRowIterator> iterators = new ArrayList<>();
            long startTime = currentTimeMillis();
            long totalTimeoutMillis = DatabaseDescriptor.getRpcTimeout(TimeUnit.MILLISECONDS);

            for (Future<UnfilteredRowIterator> f : futures)
            {
                try
                {
                    long elapsedTime = currentTimeMillis() - startTime;
                    long remainingTime = totalTimeoutMillis - elapsedTime;

                    if (remainingTime <= 0)
                    {
                        // No time left, cleanup and return empty
                        cleanupAndReportMetrics(iterators, readContext, tableMetrics);
                        return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
                    }

                    UnfilteredRowIterator itr = f.get(remainingTime, TimeUnit.MILLISECONDS);
                    if (itr != null)
                        iterators.add(itr);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    cleanupAndReportMetrics(iterators, readContext, tableMetrics);
                    throw new InvalidS3Exception("Interrupted while awaiting row iterator", e);
                }
                catch (TimeoutException e)
                {
                    // request was load shed, cleanup and return empty
                    cleanupAndReportMetrics(iterators, readContext, tableMetrics);
                    return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
                }
                catch (Throwable e)
                {
                    cleanupAndReportMetrics(iterators, readContext, tableMetrics);
                    throw new InvalidS3Exception("Error executing row iterator task", e);
                }
            }
            if (exceptionRef.get() != null)
            {
                cleanupAndReportMetrics(iterators, readContext, tableMetrics);
                throw new InvalidS3Exception(exceptionRef.get().getMessage(), exceptionRef.get());
            }

            // Report metrics before returning
            readContext.reportToTableMetrics(tableMetrics);
            if (iterators.isEmpty())
                return UnfilteredRowIterators.noRowsIterator(metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, reversed);
            return UnfilteredRowIterators.merge(iterators);
        }
        catch (Throwable e)
        {
            // upstream can end up swallowing this making debugging difficult
            NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 1, TimeUnit.MINUTES,
                             "Error executing row iterator task on {} in {}.{}", key, this.metadata().keyspace, this.metadata().name, e);
            throw e;
        }
    }

    /**
     * Report metrics and close any collected iterators. Used for cleanup on error/timeout paths.
     */
    private void cleanupAndReportMetrics(List<UnfilteredRowIterator> iterators,
                                         ReadContext readContext,
                                         TableMetrics tableMetrics)
    {
        readContext.reportToTableMetrics(tableMetrics);
        try
        {
            FBUtilities.closeAll(iterators);
        }
        catch (Exception e)
        {
            logger.warn("Failed to close iterators during cleanup", e);
        }
    }

    @Override
    public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter, DataRange dataRange, SSTableReadsListener listener)
    {
        throw new InvalidRequestException("Range queries to BackupMemtable are not allowed");
    }

    @Override
    public long getMinTimestamp()
    {
        return 0;
    }

    @Override
    public int getMinLocalDeletionTime()
    {
        return 0;
    }
}
