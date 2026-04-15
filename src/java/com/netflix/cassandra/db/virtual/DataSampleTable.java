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
package com.netflix.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * A virtual table that samples raw cell values from SSTables within a lookback window.
 * <p>
 * Each returned row contains a single raw cell value from the target table in its serialized
 * bytes representation. This is intended for building compression dictionaries — the sampled
 * values are representative of what is stored on disk.
 * <p>
 * Usage:
 * <pre>
 * SELECT * FROM netflix_views.data_sample
 * WHERE keyspace_name = 'my_ks'
 *   AND table_name = 'my_table'
 *   AND bytes_limit = '1MiB'
 *   AND lookback = '10d'
 * </pre>
 */
public class DataSampleTable extends AbstractVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(DataSampleTable.class);

    private static final String TABLE_NAME = "data_sample";
    private static final String KEYSPACE_NAME_COL = "keyspace_name";
    private static final String TABLE_NAME_COL = "table_name";
    private static final String BYTES_LIMIT_COL = "bytes_limit";
    private static final String LOOKBACK_COL = "lookback";
    private static final String VALUE_INDEX_COL = "value_index";
    private static final String DATA_COL = "data";

    private static final long MAX_BYTES_LIMIT = 1024 * 1024; // 1 MiB

    public DataSampleTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Samples raw cell values from SSTables within a lookback window")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance,
                                                                                       UTF8Type.instance,
                                                                                       UTF8Type.instance,
                                                                                       UTF8Type.instance)))
                           .addPartitionKeyColumn(KEYSPACE_NAME_COL, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE_NAME_COL, UTF8Type.instance)
                           .addPartitionKeyColumn(BYTES_LIMIT_COL, UTF8Type.instance)
                           .addPartitionKeyColumn(LOOKBACK_COL, UTF8Type.instance)
                           .addClusteringColumn(VALUE_INDEX_COL, Int32Type.instance)
                           .addRegularColumn(DATA_COL, BytesType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        throw new InvalidRequestException("All partition key fields (keyspace_name, table_name, bytes_limit, lookback) must be specified");
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        ByteBuffer[] key = ((CompositeType) metadata.partitionKeyType).split(partitionKey.getKey());
        String keyspaceName = UTF8Type.instance.getString(key[0]);
        String tableName = UTF8Type.instance.getString(key[1]);
        String bytesLimitStr = UTF8Type.instance.getString(key[2]);
        String lookbackStr = UTF8Type.instance.getString(key[3]);

        long bytesLimit;
        try
        {
            bytesLimit = new DataStorageSpec.LongBytesBound(bytesLimitStr).toBytes();
        }
        catch (Exception e)
        {
            throw new InvalidRequestException("Invalid bytes_limit '" + bytesLimitStr + "': " + e.getMessage());
        }

        if (bytesLimit > MAX_BYTES_LIMIT)
            throw new InvalidRequestException("bytes_limit exceeds maximum of 1MiB");

        long lookbackMillis;
        try
        {
            lookbackMillis = new DurationSpec.LongMillisecondsBound(lookbackStr).toMilliseconds();
        }
        catch (Exception e)
        {
            throw new InvalidRequestException("Invalid lookback '" + lookbackStr + "': " + e.getMessage());
        }

        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (ksm == null)
            throw new InvalidRequestException("Keyspace " + keyspaceName + " does not exist");
        if (ksm.isVirtual())
            throw new InvalidRequestException("Cannot sample virtual keyspace " + keyspaceName);

        TableMetadata targetTableMetadata = ksm.getTableOrViewNullable(tableName);
        if (targetTableMetadata == null)
            throw new InvalidRequestException("Table " + tableName + " does not exist in keyspace " + keyspaceName);

        ColumnFamilyStore cfs = Keyspace.openAndGetStore(targetTableMetadata);

        SimpleDataSet result = new SimpleDataSet(metadata());
        sampleData(result, cfs, targetTableMetadata, bytesLimit, lookbackMillis,
                   keyspaceName, tableName, bytesLimitStr, lookbackStr);
        return result;
    }

    /**
     * Mutable accumulator passed through the sampling methods to track progress and build the result set.
     */
    static class SampleAccumulator
    {
        final SimpleDataSet result;
        final String keyspaceName;
        final String tableName;
        final String bytesLimitStr;
        final String lookbackStr;
        final long bytesLimit;

        long accumulatedBytes;
        int valueIndex;

        SampleAccumulator(SimpleDataSet result, long bytesLimit,
                          String keyspaceName, String tableName, String bytesLimitStr, String lookbackStr)
        {
            this.result = result;
            this.bytesLimit = bytesLimit;
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.bytesLimitStr = bytesLimitStr;
            this.lookbackStr = lookbackStr;
        }

        boolean isFull()
        {
            return accumulatedBytes >= bytesLimit;
        }

        void addValue(ByteBuffer value)
        {
            accumulatedBytes += value.remaining();
            result.row(keyspaceName, tableName, bytesLimitStr, lookbackStr, valueIndex++)
                  .column(DATA_COL, ByteBufferUtil.clone(value));
        }
    }

    private void sampleData(SimpleDataSet result, ColumnFamilyStore cfs, TableMetadata targetTableMetadata,
                            long bytesLimit, long lookbackMillis,
                            String keyspaceName, String tableName, String bytesLimitStr, String lookbackStr)
    {
        long cutoffTime = Clock.Global.currentTimeMillis() - lookbackMillis;

        List<SSTableReader> recentSSTables = filterRecentSSTables(cfs, cutoffTime);
        if (recentSSTables.isEmpty())
            return;

        // Use long to avoid int overflow when summing across many SSTables
        long totalSummaryEntries = 0;
        for (SSTableReader sstable : recentSSTables)
            totalSummaryEntries += sstable.indexSummary.size();

        SampleAccumulator accumulator = new SampleAccumulator(result, bytesLimit,
                                                              keyspaceName, tableName, bytesLimitStr, lookbackStr);
        ColumnFilter columnFilter = ColumnFilter.all(targetTableMetadata);
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int nowInSec = FBUtilities.nowInSeconds();

        // Cap iterations to prevent runaway loops when partitions are all empty/tombstoned.
        int maxAttempts = (int) Math.min(10_000, Math.max(totalSummaryEntries, 1000));

        // Wall clock deadline: stop if we exceed the read timeout, even if the client has
        // already disconnected, to avoid burning server resources indefinitely.
        long deadlineNanos = Clock.Global.nanoTime() + DatabaseDescriptor.getReadRpcTimeout(TimeUnit.NANOSECONDS);

        for (int attempt = 0; attempt < maxAttempts && !accumulator.isFull(); attempt++)
        {
            if (Clock.Global.nanoTime() > deadlineNanos)
                break;
            sampleOnePartition(recentSSTables, totalSummaryEntries, rng, columnFilter, nowInSec, accumulator);
        }
    }

    /**
     * Returns live SSTables whose newest data is more recent than the cutoff time,
     * filtered to those with non-empty index summaries.
     */
    private List<SSTableReader> filterRecentSSTables(ColumnFamilyStore cfs, long cutoffTime)
    {
        Set<SSTableReader> liveSSTables = cfs.getLiveSSTables();
        List<SSTableReader> recentSSTables = new ArrayList<>(liveSSTables.size());
        for (SSTableReader sstable : liveSSTables)
        {
            if (sstable.maxDataAge > cutoffTime && sstable.indexSummary.size() > 0)
                recentSSTables.add(sstable);
        }
        return recentSSTables;
    }

    /**
     * Picks a random SSTable (weighted by index summary size), selects a random partition
     * key from its index summary, and collects live cell values into the accumulator.
     */
    private void sampleOnePartition(List<SSTableReader> recentSSTables, long totalSummaryEntries,
                                    ThreadLocalRandom rng, ColumnFilter columnFilter,
                                    int nowInSec, SampleAccumulator accumulator)
    {
        // Weighted random SSTable selection: pick a random position in the total summary space
        long target = rng.nextLong(totalSummaryEntries);
        SSTableReader sstable = null;
        int keyIndex = 0;
        long cumulative = 0;
        for (SSTableReader candidate : recentSSTables)
        {
            cumulative += candidate.indexSummary.size();
            if (target < cumulative)
            {
                sstable = candidate;
                keyIndex = (int) (target - (cumulative - candidate.indexSummary.size()));
                break;
            }
        }

        // Acquire a ref to prevent the SSTable from being cleaned up by compaction
        // while we read from it. tryRef() returns null if already released.
        Ref<SSTableReader> ref = sstable.tryRef();
        if (ref == null)
            return;

        try
        {
            byte[] keyBytes = sstable.indexSummary.getKey(keyIndex);
            DecoratedKey dk = sstable.decorateKey(ByteBuffer.wrap(keyBytes));

            try (UnfilteredRowIterator unfilteredRows = sstable.rowIterator(dk, Slices.ALL, columnFilter,
                                                                            false, SSTableReadsListener.NOOP_LISTENER);
                 RowIterator partition = UnfilteredRowIterators.filter(unfilteredRows, nowInSec))
            {
                collectCellValues(partition, accumulator);
            }
        }
        catch (CorruptSSTableException e)
        {
            logger.warn("Skipping corrupt SSTable {} during data sampling", sstable.descriptor, e);
        }
        finally
        {
            ref.release();
        }
    }

    /**
     * Iterates through all rows in a partition, collecting each live cell value into the accumulator
     * until the byte limit is reached.
     */
    void collectCellValues(RowIterator partition, SampleAccumulator accumulator)
    {
        while (partition.hasNext() && !accumulator.isFull())
        {
            Row row = partition.next();
            for (ColumnData cd : row)
            {
                if (accumulator.isFull())
                    break;

                if (cd instanceof ComplexColumnData)
                {
                    for (Cell<?> cell : (ComplexColumnData) cd)
                    {
                        if (accumulator.isFull())
                            break;
                        ByteBuffer value = cell.buffer();
                        if (value.remaining() > 0)
                            accumulator.addValue(value);
                    }
                }
                else if (cd instanceof Cell)
                {
                    ByteBuffer value = ((Cell<?>) cd).buffer();
                    if (value.remaining() > 0)
                        accumulator.addValue(value);
                }
            }
        }
    }
}
