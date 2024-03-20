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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
public class DistributedJsonTable implements VirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(DistributedJsonTable.class);
    private static final int TIMEOUT_MS = 10000;

    protected final TableMetadata metadata;
    protected DistributedJsonTable(String keyspace)
    {
        metadata = TableMetadata.builder(keyspace, "cluster_view")
                           .comment("user submitted partition compaction tasks")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn("keyspace_name", UTF8Type.instance)
                           .addPartitionKeyColumn("table_name", UTF8Type.instance)
                           .addClusteringColumn("host", UTF8Type.instance)
                           .addRegularColumn("value", UTF8Type.instance)
                           .build();
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Updates are not supported by table " + metadata);
    }

    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter)
    {
        ByteBuffer[] key = ((CompositeType) this.metadata.partitionKeyType).split(partitionKey.getKey());
        String keyspace = UTF8Type.instance.getString(key[0]);
        String table = UTF8Type.instance.getString(key[1]);
        logger.info("DistributedJsonTable submitted: " + keyspace + '.' + table);

        // to include virtual tables need to pull from both Schema.instance and VirtualKeyspaceRegistry
        VirtualKeyspace vk = VirtualKeyspaceRegistry.instance.getKeyspaceNullable(keyspace);
        TableMetadata target;
        if (vk == null)
        {
            target = Schema.instance.getTableMetadata(keyspace, table);
            if (target == null)
                throw new InvalidRequestException("Table " + keyspace + '.' + table + " does not exist");
        }
        else
        {
            VirtualTable vt = vk.tables().stream().filter(t -> t.metadata().name.equals(table)).findFirst().orElse(null);
            if (vt == null)
                throw new InvalidRequestException("Table " + keyspace + '.' + table + " does not exist");
            target = vt.metadata();
        }

        PartitionRangeReadCommand read = PartitionRangeReadCommand.allDataRead(target, FBUtilities.nowInSeconds());
        Set<InetAddressAndPort> allEndpoints = StorageService.instance.getLiveRingMembers(true);

        Map<InetAddressAndPort, Future<Message<ReadResponse>>> results = Maps.newHashMap();
        for (InetAddressAndPort endpoint : allEndpoints)
        {
            Message<ReadCommand> m = Message.out(read.verb(), read);
            results.put(endpoint, MessagingService.instance().<ReadResponse>sendWithResult(m, endpoint));
        }
        long startTime = Clock.Global.currentTimeMillis();

        // Wait for all futures to complete or timeout to occur
        boolean allDone;
        do {
            allDone = true;
            for (Future<?> future : results.values()) {
                if (!future.isDone() && Clock.Global.currentTimeMillis() - startTime < TIMEOUT_MS) {
                    allDone = false;
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    break;
                }
            }
        } while (!allDone && Clock.Global.currentTimeMillis() - startTime < TIMEOUT_MS);

        Iterator<Map.Entry<InetAddressAndPort, Future<Message<ReadResponse>>>> iterator = results.entrySet().iterator();
        UnfilteredRowIterator rows = new AbstractUnfilteredRowIterator(metadata,
                                                 partitionKey,
                                                 DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(),
                                                 Rows.EMPTY_STATIC_ROW,
                                                 false,
                                                 EncodingStats.NO_STATS)
        {
            @Override
            protected Unfiltered computeNext()
            {
                if (iterator.hasNext())
                {
                    Map.Entry<InetAddressAndPort, Future<Message<ReadResponse>>> entry = iterator.next();
                    if (entry.getValue().isDone())
                    {
                        try
                        {
                            Message<ReadResponse> message = entry.getValue().get();
                            StringBuilder sb = new StringBuilder();
                            sb.append('[');
                            message.payload.makeIterator(read).forEachRemaining(row -> {
                                if (row.isEmpty()) return;
                                if (sb.length() > 1)
                                    sb.append(',');
                                sb.append(toJson(target, row));
                            });
                            sb.append(']');

                            Clustering<?> cl = metadata.comparator.make(message.from().toString(true));
                            Row.Builder row = BTreeRow.sortedBuilder();
                            row.newRow(cl);
                            ByteBuffer bb = UTF8Type.instance.decompose(sb.toString());
                            ColumnMetadata def = metadata.regularColumns().getSimple(0);
                            row.addCell(new BufferCell(def, 1L, BufferCell.NO_TTL, BufferCell.NO_DELETION_TIME, bb, null));
                            return row.build();
                        }
                        catch (Exception e)
                        {
                            logger.error("Error reading from " + entry.getValue(), e);
                        }
                    }
                    else
                    {
                        Clustering<?> cl = metadata.comparator.make(entry.getKey().toString(true));
                        Row.Builder row = BTreeRow.sortedBuilder();
                        row.newRow(cl);
                        ColumnMetadata def = metadata.regularColumns().getSimple(0);

                        ByteBuffer bb = UTF8Type.instance.decompose("here");
                        BufferCell buf = new BufferCell(def, 1L, BufferCell.NO_TTL, BufferCell.NO_DELETION_TIME, bb, null);
                        row.addCell(buf);
                        return row.build();
                    }
                } else {
                    return endOfData();
                }
                return null;
            }
        };

        return new SingletonUnfilteredPartitionIterator(rows);
    }

    @Override
    public UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter)
    {
        throw new InvalidRequestException("Range queries are not supported by table, keyspace_name and table_name must be included in query" + metadata);
    }

    public String toJson(TableMetadata src, UnfilteredRowIterator row) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');

        boolean needComma = false; // Flag to indicate whether a comma is needed before appending the next item

        while (row.hasNext()) {
            Row r = (Row) row.next();

            // Process partition key columns
            for (ColumnMetadata c : src.partitionKeyColumns()) {
                if (needComma) {
                    sb.append(", ");
                }
                sb.append('"' + c.name.toCQLString() + "\": " + c.type.toJSONString(row.partitionKey().getKey(), ProtocolVersion.CURRENT));
                needComma = true; // After appending an item, set needComma to true
            }

            // Process clustering columns, if any
            for (ColumnMetadata c : src.clusteringColumns()) {
                if (needComma) {
                    sb.append(", ");
                }
                sb.append('"' + c.name.toCQLString() + "\": " + c.type.toJSONString(r.clustering().bufferAt(c.position()), ProtocolVersion.CURRENT));
                needComma = true;
            }

            // Process other columns, if any
            for (ColumnData cd : r) {
                if (needComma) {
                    sb.append(", ");
                }
                String json = cd.column().type.toJSONString(r.getCell(cd.column()).buffer(), ProtocolVersion.CURRENT);
                sb.append('"' + cd.column().name.toCQLString() + "\": " + json);
                needComma = true;
            }
        }

        sb.append('}');
        return sb.toString();
    }

    @Override
    public void truncate()
    {
        throw new InvalidRequestException("Truncation is not supported by table " + metadata);
    }
}
