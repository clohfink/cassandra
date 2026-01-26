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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.backups.BackupContext;
import com.netflix.cassandra.backups.BackupManifest;
import com.netflix.cassandra.backups.BackupUtils;
import com.netflix.cassandra.backups.ObjectStoreAccess;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Virtual table that shows detailed information about sstables and components in a specific backup.
 * This table requires keyspace, table, and timestamp to be specified in queries.
 * <p>
 * Columns:
 * - keyspace_name (partition key): The keyspace name (required)
 * - table_name (partition key): The table name (required)
 * - timestamp (partition key): The backup timestamp in milliseconds (required)
 * - sstable_prefix (clustering key): The sstable prefix/name
 * - component_name (clustering key): The component file name
 * - file_size: Size of the component file in bytes
 * - uploaded: True if the component has been uploaded
 * - compression: Compression algorithm used
 * - encryption: Encryption algorithm used
 * - backup_path: S3 path to the backed up file
 * <p>
 * Example:
 * <pre>
 * cqlsh> expand on
 * Now Expanded output is enabled
 * cqlsh> select * from netflix_views.backup_details where keyspace_name = 'casscoldtier_1d_2rf' and table_name = 'data_20251208_12_43' and timestamp = 1765215000000 ;
 *
 * @ Row 1
 * ----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 *  keyspace_name  | casscoldtier_1d_2rf
 *  table_name     | data_20251208_12_43
 *  timestamp      | 1765215000000
 *  sstable_prefix | nb-4-big
 *  component_name | nb-4-big-CompressionInfo.db
 *  backup_path    | test_backup/5108_cass_perf_cold_tier_experiment/-3074457343809683002/SST_V2/1765214399000/casscoldtier_1d_2rf/data_20251208_12_43-3445b703ead13f6b8a0f9439391aa42b/NONE/PLAINTEXT/nb-4-big-CompressionInfo.db
 *  compression    | NONE
 *  encryption     | PLAINTEXT
 *  file_size      | 78
 *  uploaded       | False
 *
 * @ Row 2
 * ----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 *  keyspace_name  | casscoldtier_1d_2rf
 *  table_name     | data_20251208_12_43
 *  timestamp      | 1765215000000
 *  sstable_prefix | nb-4-big
 *  component_name | nb-4-big-Data.db
 *  backup_path    | test_backup/5108_cass_perf_cold_tier_experiment/-3074457343809683002/SST_V2/1765214399000/casscoldtier_1d_2rf/data_20251208_12_43-3445b703ead13f6b8a0f9439391aa42b/NONE/PLAINTEXT/nb-4-big-Data.db
 *  compression    | NONE
 *  encryption     | PLAINTEXT
 *  file_size      | 954930
 *  uploaded       | False
 *  ...
 * </pre>
 */
public class BackupDetailsTable extends ScopedTable
{
    private static final Logger logger = LoggerFactory.getLogger(BackupDetailsTable.class);

    public static final String TABLE_NAME = "backup_details";

    private static final String KEYSPACE = "keyspace_name";
    private static final String TABLE = "table_name";
    private static final String TIMESTAMP = "timestamp";
    private static final String SSTABLE_PREFIX = "sstable_prefix";
    private static final String COMPONENT_NAME = "component_name";
    private static final String FILE_SIZE = "file_size";
    private static final String UPLOADED = "uploaded";
    private static final String COMPRESSION = "compression";
    private static final String ENCRYPTION = "encryption";
    private static final String BACKUP_PATH = "backup_path";
    private final BackupContext backupContext;
    private final ObjectStoreAccess objectStore;

    public BackupDetailsTable(String keyspace, BackupContext backupContext, ObjectStoreAccess objectStore)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Detailed sstable and component information for a specific backup (requires keyspace, table, and timestamp)")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance, LongType.instance)))
                           .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE, UTF8Type.instance)
                           .addPartitionKeyColumn(TIMESTAMP, LongType.instance)
                           .addClusteringColumn(SSTABLE_PREFIX, UTF8Type.instance)
                           .addClusteringColumn(COMPONENT_NAME, UTF8Type.instance)
                           .addRegularColumn(FILE_SIZE, LongType.instance)
                           .addRegularColumn(UPLOADED, BooleanType.instance)
                           .addRegularColumn(COMPRESSION, UTF8Type.instance)
                           .addRegularColumn(ENCRYPTION, UTF8Type.instance)
                           .addRegularColumn(BACKUP_PATH, UTF8Type.instance)
                           .build());
        this.backupContext = backupContext;
        this.objectStore = objectStore;
    }

    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter,
                                              ColumnFilter columnFilter, RowFilter rowFilter)
    {
        // Override the parent method to pass filters through
        ByteBuffer[] key = ((CompositeType) this.metadata.partitionKeyType).split(partitionKey.getKey());
        String keyspace = UTF8Type.instance.getString(key[0]);
        String table = UTF8Type.instance.getString(key[1]);
        long timestamp = LongType.instance.compose(key[2]);

        // Verify keyspace and table exists (same as parent ScopedTable)
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        if (ksm == null)
        {
            throw new InvalidRequestException("Keyspace " + keyspace + " does not exist");
        }
        TableMetadata metadata = ksm.getTableOrViewNullable(table);
        if (metadata == null)
        {
            throw new InvalidRequestException("Table " + table + " does not exist in keyspace " + keyspace);
        }

        return new SingletonUnfilteredPartitionIterator(selectInternal(partitionKey, keyspace, table, timestamp, clusteringIndexFilter, columnFilter));
    }

    @Override
    public UnfilteredRowIterator select(DecoratedKey partitionKey, String keyspace, String table)
    {
        // This signature doesn't have timestamp, but we need it
        // Extract timestamp from partition key
        ByteBuffer[] key = ((CompositeType) this.metadata.partitionKeyType).split(partitionKey.getKey());
        long timestamp = LongType.instance.compose(key[2]);
        return selectInternal(partitionKey, keyspace, table, timestamp, null, null);
    }

    private UnfilteredRowIterator selectInternal(DecoratedKey partitionKey, String keyspace, String table,
                                                 long timestamp, ClusteringIndexFilter clusteringFilter,
                                                 ColumnFilter columnFilter)
    {
        SimpleDataSet result = new SimpleDataSet(metadata);
        if (!backupContext.isValid())
        {
            logger.warn("Not all required environment variables are present and valid for backups details table: NETFLIX_REGION={}, NETFLIX_APP={}, NETFLIX_ENVIRONMENT={}",
                        backupContext.region(), backupContext.app(), backupContext.env());
            return BackupUtils.toRowIterator(metadata, result, partitionKey, clusteringFilter, columnFilter);
        }
        List<String> keys;
        List<Future<byte[]>> manifestFutures;
        try
        {
            keys = objectStore.getObjectKeys(backupContext.bucket(), backupContext.metafilePrefix())
                                           .get()
                                           .stream()
                                           .filter(key -> BackupUtils.extractTimestampFromKey(key) == timestamp)
                                           .toList();
            manifestFutures = keys.stream()
                                  .map(key -> objectStore.getObjectAsBytes(backupContext.bucket(), key))
                                  .collect(Collectors.toList());
        }
        catch (Exception e)
        {
            logger.error("Failed to get backup details for {}.{} at timestamp {}", keyspace, table, timestamp, e);
            return BackupUtils.toRowIterator(metadata, result, partitionKey, clusteringFilter, columnFilter);
        }

        // Process each manifest
        for (int i = 0; i < keys.size(); i++)
        {
            String key = keys.get(i);
            Optional<BackupManifest> manifest = BackupUtils.getManifest(manifestFutures.get(i));
            if (manifest.isEmpty())
            {
                continue;
            }
            try
            {
                manifest.get().getData(keyspace, table)
                        .stream()
                        .flatMap(tableData -> tableData.getSstables().stream())
                        .forEach(sstable ->
                                 {
                                     String sstablePrefix = sstable.getPrefix();
                                     sstable.getSstableComponents().stream()
                                            .filter(component -> !BackupUtils.isMetadataFile(component.getFileName()))
                                            .forEach(component ->
                                                     result.row(keyspace, table, timestamp, sstablePrefix, component.getFileName())
                                                           .column(FILE_SIZE, component.getFileSizeOnDisk())
                                                           .column(UPLOADED, component.isUploaded())
                                                           .column(COMPRESSION, component.getCompression())
                                                           .column(ENCRYPTION, component.getEncryption())
                                                           .column(BACKUP_PATH, component.getBackupPath()));
                                 });
            }
            catch (Exception e)
            {
                logger.warn("Failed to process backup metadata file: {}", key, e);
            }
        }
        return BackupUtils.toRowIterator(metadata, result, partitionKey, clusteringFilter, columnFilter);
    }
}
