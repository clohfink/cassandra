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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * ClusterPartitionCount is a virtual table that provides an estimate of the total number of partitions
 * in the cluster, with tombstones unresolved.
 */
public class ClusterPartitionCount extends ScopedTable
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterPartitionCount.class);
    public static final String NAME = "partition_count";
    private Cache<String, ICardinality> cache;

    public ClusterPartitionCount(String keyspace)
    {
        super(TableMetadata.builder(keyspace, NAME)
                           .comment("Estimate of total number of partitions in the cluster, tombstones unresolved")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn("keyspace_name", UTF8Type.instance)
                           .addPartitionKeyColumn("table_name", UTF8Type.instance)
                           .addRegularColumn("count", LongType.instance)
                           .build());
        cache = CacheBuilder.newBuilder()
                            .expireAfterWrite(DatabaseDescriptor.getPartitionCountCacheExpiryMinutes(), TimeUnit.MINUTES)
                            .build();
    }

    /**
     * Updates the cache expiry time.
     *
     * @param minutes The new expiry time in minutes.
     */
    public void updateCacheExpiryTime(int minutes)
    {
        cache = CacheBuilder.newBuilder()
                            .expireAfterWrite(minutes, TimeUnit.MINUTES)
                            .build();
    }

    /**
     * Selects the partition count for the given partition key, keyspace, and table.
     * Results are cached for 30 minutes. The partition count does not resolve tombstones.
     *
     * @param partitionKey The partition key.
     * @param keyspace     The keyspace name.
     * @param table        The table name.
     * @return An UnfilteredRowIterator containing the partition count.
     */
    @Override
    public synchronized UnfilteredRowIterator select(DecoratedKey partitionKey, String keyspace, String table)
    {
        String cacheKey = keyspace + '.' + table;
        ICardinality cachedResult = cache.getIfPresent(cacheKey);
        if (cachedResult != null)
        {
            return createRowIterator(partitionKey, cachedResult);
        }
        SinglePartitionReadCommand read = createReadCommand(partitionKey);
        Map<InetAddressAndPort, Future<Message<ReadResponse>>> results = sendReadCommandToAllEndpoints(read);

        // Track completed racks
        Map<String, Set<InetAddressAndPort>> rackResponses = new HashMap<>();
        Map<String, Set<InetAddressAndPort>> rackNodes = new HashMap<>();
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

        // First pass to identify all racks and their nodes
        for (InetAddressAndPort endpoint : results.keySet())
        {
            String rack = snitch.getRack(endpoint);
            rackNodes.computeIfAbsent(rack, k -> new HashSet<>()).add(endpoint);
        }

        waitForFullRack(results, rackNodes, rackResponses, snitch);

        // see MetadataCollector.cardinality
        ICardinality base = new HyperLogLogPlus(13, 25);
        try
        {
            base = processResults(results, read, base);
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        UnfilteredRowIterator result = createRowIterator(partitionKey, base);
        cache.put(cacheKey, base);

        return result;
    }

    /**
     * Waits for responses from a full rack of nodes.
     *
     * @param results The map of results from each endpoint
     * @param rackNodes Map of rack names to their nodes
     * @param rackResponses Map to track which nodes in each rack have responded
     * @param snitch The endpoint snitch to get rack information
     * @return true if a full rack was found, false if timeout occurred
     */
    public boolean waitForFullRack(Map<InetAddressAndPort, Future<Message<ReadResponse>>> results,
                                   Map<String, Set<InetAddressAndPort>> rackNodes,
                                   Map<String, Set<InetAddressAndPort>> rackResponses,
                                   IEndpointSnitch snitch)
    {
        long startTime = Clock.Global.currentTimeMillis();
        long timeout = DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MILLISECONDS) / 2;
        boolean hasFullRack = false;

        while (!hasFullRack && Clock.Global.currentTimeMillis() - startTime < timeout)
        {
            for (Map.Entry<InetAddressAndPort, Future<Message<ReadResponse>>> entry : results.entrySet())
            {
                if (entry.getValue().isDone())
                {
                    String rack = snitch.getRack(entry.getKey());
                    rackResponses.computeIfAbsent(rack, k -> new HashSet<>()).add(entry.getKey());

                    // Check if we have a full rack
                    Set<InetAddressAndPort> rackNodesSet = rackNodes.get(rack);
                    Set<InetAddressAndPort> rackResponsesSet = rackResponses.get(rack);
                    if (rackNodesSet != null && rackResponsesSet != null &&
                        rackNodesSet.size() == rackResponsesSet.size())
                    {
                        hasFullRack = true;
                        break;
                    }
                }
            }

            if (!hasFullRack)
            {
                try
                {
                    Thread.sleep(10); // Small sleep to prevent busy waiting
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        return hasFullRack;
    }

    /**
     * Creates a SinglePartitionReadCommand for the given HLL table and partition key.
     *
     * @param partitionKey The partition key.
     * @return A SinglePartitionReadCommand.
     */
    private SinglePartitionReadCommand createReadCommand(DecoratedKey partitionKey)
    {
        VirtualTable hllTable = NetflixViewsKeyspace.instance.getTable(TableHLL.NAME);
        return SinglePartitionReadCommand.fullPartitionRead(hllTable.metadata(), FBUtilities.nowInSeconds(), partitionKey);
    }

    /**
     * Processes the results of the read command and merges the HyperLogLog data.
     *
     * @param results The map of results from each endpoint.
     * @param read    The read command.
     * @param hll     The base HyperLogLog instance to merge into.
     */
    private ICardinality processResults(Map<InetAddressAndPort, Future<Message<ReadResponse>>> results, SinglePartitionReadCommand read, ICardinality hll) throws ExecutionException, InterruptedException
    {
        for (Map.Entry<InetAddressAndPort, Future<Message<ReadResponse>>> entry : results.entrySet())
        {
            // Entry is the ip source, and the should be completed future for result message
            if (entry.getValue().isDone())
            {
                Message<ReadResponse> message = entry.getValue().get();
                try (UnfilteredPartitionIterator readResult = message.payload.makeIterator(read))
                {
                    // each node should have a single partition, enforced in HLL table
                    if (readResult.hasNext())
                    {
                        UnfilteredRowIterator partition = readResult.next();
                        // there is no clustering key so single row as well
                        if (partition.hasNext())
                        {
                            Unfiltered row = partition.next();
                            // it only returns Rows so safe cast, no Tombstone markers in HLL VT
                            ByteBuffer bb = ((Row) row).cells().iterator().next().buffer();
                            hll = hll.merge(HyperLogLogPlus.Builder.build(bb.array()));
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.error("Error reading from " + entry.getValue(), e);
                }
            }
        }
        return hll;
    }

    /**
     * Creates the resulting UnfilteredRowIterator for the given partition key and HyperLogLog data.
     *
     * @param partitionKey The partition key.
     * @param base         The HyperLogLog instance containing the partition count.
     * @return An UnfilteredRowIterator.
     */
    private UnfilteredRowIterator createRowIterator(DecoratedKey partitionKey, ICardinality base)
    {
        long count = base.cardinality();
        ByteBuffer bb = LongType.instance.decompose(count);
        ColumnMetadata def = metadata.regularColumns().getSimple(0);
        BufferCell cell = new BufferCell(def, 1L, BufferCell.NO_TTL, BufferCell.NO_DELETION_TIME, bb, null);

        Row.Builder row = BTreeRow.sortedBuilder();
        row.newRow(Clustering.EMPTY);
        row.addCell(cell);
        return UnfilteredRowIterators.singleton(row.build(), metadata(), partitionKey, DeletionTime.LIVE,
                                                metadata.regularAndStaticColumns(),
                                                Rows.EMPTY_STATIC_ROW, false,
                                                EncodingStats.NO_STATS);
    }
}
