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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.backups.BackupUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Virtual table that shows backup completeness across all nodes in the cluster.
 * <p>
 * Unlike {@link BackupsTable} which only shows the local node's token, this table
 * fans out a read of {@code netflix_views.backups} to every node in the ring
 * (like {@link DistributedJsonTable}), then aggregates upload completeness per timestamp.
 * <p>
 * This enables finding valid restore points where all nodes have completed their backup.
 * <p>
 * Columns:
 * - keyspace_name (partition key): The keyspace name (required)
 * - table_name (partition key): The table name (required)
 * - timestamp (clustering key): The backup timestamp in milliseconds
 * - total_size: Total size of all backup files across all nodes in bytes
 * - num_tokens: Number of nodes with data for this timestamp
 * - num_uploaded: Number of nodes where all components are uploaded
 * - uploaded: True when num_tokens == num_uploaded (all nodes fully uploaded)
 * <p>
 * Example:
 * <pre>
 *  cqlsh&gt; select * from netflix_views.complete_backups where keyspace_name = 'my_ks' and table_name = 'my_table';
 *
 *  keyspace_name | table_name | timestamp     | total_size | num_tokens | num_uploaded | uploaded
 * ---------------+------------+---------------+------------+------------+--------------+----------
 *  my_ks         | my_table   | 1765211400000 |    3456789 |          3 |            3 |     True
 *  my_ks         | my_table   | 1765213200000 |    2615350 |          3 |            2 |    False
 * </pre>
 */
public class CompleteBackupsTable extends ScopedTable
{
    private static final Logger logger = LoggerFactory.getLogger(CompleteBackupsTable.class);

    public static final String TABLE_NAME = "complete_backups";

    private static final String KEYSPACE = "keyspace_name";
    private static final String TABLE = "table_name";
    private static final String TIMESTAMP = "timestamp";
    private static final String TOTAL_SIZE = "total_size";
    private static final String NUM_TOKENS = "num_tokens";
    private static final String NUM_UPLOADED = "num_uploaded";
    private static final String UPLOADED = "uploaded";
    private static final String MISSING_UPLOADS = "missing_uploads";

    public CompleteBackupsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Backup completeness across all nodes in the cluster (requires keyspace and table)")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance)))
                           .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE, UTF8Type.instance)
                           .addClusteringColumn(TIMESTAMP, LongType.instance)
                           .addRegularColumn(TOTAL_SIZE, LongType.instance)
                           .addRegularColumn(NUM_TOKENS, Int32Type.instance)
                           .addRegularColumn(NUM_UPLOADED, Int32Type.instance)
                           .addRegularColumn(UPLOADED, BooleanType.instance)
                           .addRegularColumn(MISSING_UPLOADS, SetType.getInstance(UTF8Type.instance, false))
                           .build());
    }

    @Override
    public UnfilteredRowIterator select(DecoratedKey partitionKey, String keyspace, String table)
    {
        SimpleDataSet result = new SimpleDataSet(metadata);

        // Look up the backups table metadata
        TableMetadata backupsMetadata = getBackupsTableMetadata();
        if (backupsMetadata == null)
        {
            logger.warn("Could not find backups table metadata");
            return BackupUtils.toRowIterator(metadata, result, partitionKey, null, null);
        }

        // Build a read command for the backups table with the same partition key
        int now = FBUtilities.nowInSeconds();
        DecoratedKey backupsKey = backupsMetadata.partitioner.decorateKey(partitionKey.getKey());
        SinglePartitionReadCommand read = SinglePartitionReadCommand.create(
            backupsMetadata, now, backupsKey, ColumnFilter.all(backupsMetadata),
            new ClusteringIndexSliceFilter(Slices.ALL, false));

        // Fan out to all endpoints
        Map<InetAddressAndPort, Future<Message<ReadResponse>>> responses = sendReadCommandToAllEndpoints(read);
        long startTime = Clock.Global.currentTimeMillis();
        long timeout = DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MILLISECONDS);
        waitForFutures(responses, startTime, timeout);

        // Column metadata from the backups table for extracting values
        ColumnMetadata totalSizeCol = backupsMetadata.getColumn(ColumnIdentifier.getInterned(TOTAL_SIZE, true));
        ColumnMetadata uploadedCol = backupsMetadata.getColumn(ColumnIdentifier.getInterned(UPLOADED, true));

        // Aggregate responses
        Map<Long, TimestampAggregation> aggregations = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, Future<Message<ReadResponse>>> entry : responses.entrySet())
        {
            if (!entry.getValue().isDone())
                continue;
            try
            {
                Message<ReadResponse> message = entry.getValue().get(0, TimeUnit.MILLISECONDS);
                try (PartitionIterator it = UnfilteredPartitionIterators.filter(message.payload.makeIterator(read), now))
                {
                    while (it.hasNext())
                    {
                        try (RowIterator partition = it.next())
                        {
                            while (partition.hasNext())
                            {
                                Row row = partition.next();
                                // Extract clustering column: timestamp
                                long timestamp = LongType.instance.compose(row.clustering().bufferAt(0));
                                // Extract regular columns
                                long size = row.getCell(totalSizeCol) != null
                                             ? LongType.instance.compose(row.getCell(totalSizeCol).buffer())
                                             : 0L;
                                boolean isUploaded = row.getCell(uploadedCol) != null
                                                     && BooleanType.instance.compose(row.getCell(uploadedCol).buffer());

                                TimestampAggregation agg = aggregations.computeIfAbsent(timestamp, ts -> new TimestampAggregation());
                                agg.addToken(entry.getKey(), size, isUploaded);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.warn("Failed to read backups from {}", entry.getKey(), e);
            }
        }

        // Emit rows
        for (Map.Entry<Long, TimestampAggregation> entry : aggregations.entrySet())
        {
            long timestamp = entry.getKey();
            TimestampAggregation agg = entry.getValue();
            result.row(keyspace, table, timestamp)
                  .column(TOTAL_SIZE, agg.totalSize)
                  .column(NUM_TOKENS, agg.numTokens)
                  .column(NUM_UPLOADED, agg.numUploaded)
                  .column(UPLOADED, agg.numTokens == agg.numUploaded)
                  .column(MISSING_UPLOADS, agg.missingUploads);
        }

        return BackupUtils.toRowIterator(metadata, result, partitionKey, null, null);
    }

    private TableMetadata getBackupsTableMetadata()
    {
        VirtualKeyspace vk = VirtualKeyspaceRegistry.instance.getKeyspaceNullable(NetflixViewsKeyspace.NAME);
        if (vk == null)
            return null;
        for (VirtualTable vt : vk.tables())
        {
            if (vt.metadata().name.equals(BackupsTable.TABLE_NAME))
                return vt.metadata();
        }
        return null;
    }

    private static class TimestampAggregation
    {
        long totalSize;
        int numTokens;
        int numUploaded;
        Set<String> missingUploads = new HashSet<>();

        void addToken(InetAddressAndPort endpoint, long size, boolean uploaded)
        {
            numTokens++;
            totalSize += size;
            if (uploaded)
                numUploaded++;
            else
                missingUploads.add(endpoint.getHostAddress(false));
        }
    }
}
