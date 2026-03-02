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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.backups.BackupContext;
import com.netflix.cassandra.backups.BackupManifest;
import com.netflix.cassandra.backups.BackupUtils;
import com.netflix.cassandra.backups.ObjectStoreAccess;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Virtual table that lists available backups from S3 for the current node's token.
 * This table queries S3 for backup metadata files and exposes them as rows.
 * Parsed manifests are cached using Caffeine with a configurable expiry
 * ({@code backup_manifest_cache_expiry} in cassandra.yaml, default 10m).
 * <p>
 * This is a scoped table, meaning keyspace and table must be specified in queries.
 * <p>
 * Columns:
 * - keyspace (partition key): The keyspace name (required)
 * - table (partition key): The table name (required)
 * - timestamp (clustering key): The backup timestamp in milliseconds
 * - app_name: The Netflix application name
 * - total_size: Total size of all backup files in bytes
 * - uploaded: True if all components are uploaded
 * <p>
 * Example:
 * <pre>
 *  cqlsh> select * from netflix_views.backups where keyspace_name = 'casscoldtier_1d_2rf' and table_name = 'data_20251208_12_43' ;
 *
 *  keyspace_name       | table_name          | timestamp     | app_name                       | total_size | uploaded
 * ---------------------+---------------------+---------------+--------------------------------+------------+----------
 *  casscoldtier_1d_2rf | data_20251208_12_43 | 1765211400000 | cass_perf_cold_tier_experiment |    1146049 |     True
 *  casscoldtier_1d_2rf | data_20251208_12_43 | 1765213200000 | cass_perf_cold_tier_experiment |    2615350 |     True
 *  casscoldtier_1d_2rf | data_20251208_12_43 | 1765215000000 | cass_perf_cold_tier_experiment |    2014895 |    False
 *  casscoldtier_1d_2rf | data_20251208_12_43 | 1765216800000 | cass_perf_cold_tier_experiment |    2014895 |     True
 *  casscoldtier_1d_2rf | data_20251208_12_43 | 1765218600000 | cass_perf_cold_tier_experiment |    2014895 |     True
 *  casscoldtier_1d_2rf | data_20251208_12_43 | 1765220400000 | cass_perf_cold_tier_experiment |    2014895 |     True
 * </pre>
 */
public class BackupsTable extends ScopedTable
{
    private static final Logger logger = LoggerFactory.getLogger(BackupsTable.class);

    public static final String TABLE_NAME = "backups";

    private static final String KEYSPACE = "keyspace_name";
    private static final String TABLE = "table_name";
    private static final String TIMESTAMP = "timestamp";
    private static final String APP_NAME = "app_name";
    private static final String TOTAL_SIZE = "total_size";
    private static final String UPLOADED = "uploaded";

    // Cache stores in-flight or completed futures so concurrent queries share the same S3 fetch
    private final LoadingCache<String, Future<Optional<BackupManifest>>> manifestCache;
    private final BackupContext backupContext;
    private final ObjectStoreAccess objectStore;

    public BackupsTable(String keyspace, BackupContext backupContext, ObjectStoreAccess objectStore)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Available backups from S3 (requires keyspace and table)")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance)))
                           .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE, UTF8Type.instance)
                           .addClusteringColumn(TIMESTAMP, LongType.instance)
                           .addRegularColumn(APP_NAME, UTF8Type.instance)
                           .addRegularColumn(TOTAL_SIZE, LongType.instance)
                           .addRegularColumn(UPLOADED, BooleanType.instance)
                           .build());
        this.backupContext = backupContext;
        this.objectStore = objectStore;
        this.manifestCache = buildCache();
    }

    private LoadingCache<String, Future<Optional<BackupManifest>>> buildCache()
    {
        int expiryMinutes = DatabaseDescriptor.getBackupManifestCacheExpiryMinutes();
        int maxSize = DatabaseDescriptor.getBackupManifestCacheMaxSize();
        return Caffeine.newBuilder()
                       .expireAfterWrite(expiryMinutes, TimeUnit.MINUTES)
                       .maximumSize(maxSize)
                       .build(new CacheLoader<String, Future<Optional<BackupManifest>>>()
                       {
                           @Override
                           public Future<Optional<BackupManifest>> load(String key)
                           {
                               return objectStore.getObjectAsBytes(backupContext.bucket(), key)
                                                 .map(BackupUtils::parseManifest);
                           }

                           @Override
                           public Map<String, Future<Optional<BackupManifest>>> loadAll(Iterable<? extends String> keys)
                           {
                               Map<String, Future<Optional<BackupManifest>>> results = new HashMap<>();
                               for (String key : keys)
                                   results.put(key, objectStore.getObjectAsBytes(backupContext.bucket(), key)
                                                               .map(BackupUtils::parseManifest));
                               return results;
                           }
                       });
    }

    @VisibleForTesting
    public void clearManifestCache()
    {
        manifestCache.invalidateAll();
    }

    @Override
    public UnfilteredRowIterator select(DecoratedKey partitionKey, String keyspace, String table)
    {
        return selectInternal(partitionKey, keyspace, table, null, null);
    }

    @Override
    protected UnfilteredRowIterator select(DecoratedKey partitionKey, String keyspace, String table,
                                           ClusteringIndexFilter clusteringFilter, ColumnFilter columnFilter)
    {
        return selectInternal(partitionKey, keyspace, table, clusteringFilter, columnFilter);
    }

    private UnfilteredRowIterator selectInternal(DecoratedKey partitionKey, String keyspace, String table,
                                                 ClusteringIndexFilter clusteringFilter, ColumnFilter columnFilter)
    {
        SimpleDataSet result = new SimpleDataSet(metadata);
        if (!backupContext.isValid())
        {
            logger.warn("Not all required environment variables are present and valid for backups table: NETFLIX_REGION={}, NETFLIX_APP={}, NETFLIX_ENVIRONMENT={}",
                        backupContext.region(), backupContext.app(), backupContext.env());
            return BackupUtils.toRowIterator(metadata, result, partitionKey, clusteringFilter, columnFilter);
        }

        List<String> keys;
        try
        {
            keys = objectStore.getObjectKeys(backupContext.bucket(), backupContext.metafilePrefix()).get();
        }
        catch (Exception e)
        {
            logger.error("Failed to list backups for {}.{}", keyspace, table, e);
            return BackupUtils.toRowIterator(metadata, result, partitionKey, clusteringFilter, columnFilter);
        }

        // getAll returns immediately for cached entries; uncached entries trigger parallel S3 fetches via loadAll
        Map<String, Future<Optional<BackupManifest>>> futures = manifestCache.getAll(keys);
        long deadlineNanos = Clock.Global.nanoTime() + TimeUnit.SECONDS.toNanos(DatabaseDescriptor.getBackupManifestFetchTimeoutSeconds());

        for (String key : keys)
        {
            long timestamp = BackupUtils.extractTimestampFromKey(key);
            if (timestamp == -1)
                continue;

            long remainingNanos = deadlineNanos - Clock.Global.nanoTime();
            if (remainingNanos <= 0)
            {
                logger.warn("Deadline exceeded fetching backup manifests, skipping remaining {} keys", keys.size() - keys.indexOf(key));
                break;
            }

            Optional<BackupManifest> manifest;
            try
            {
                manifest = futures.get(key).get(remainingNanos, TimeUnit.NANOSECONDS);
            }
            catch (TimeoutException e)
            {
                logger.warn("Timed out fetching backup manifests, skipping remaining keys");
                manifestCache.invalidate(key);
                break;
            }
            catch (Exception e)
            {
                logger.warn("Failed to fetch backup manifest: {}", key, e);
                manifestCache.invalidate(key);
                continue;
            }

            if (manifest.isEmpty())
                continue;

            try
            {
                String appName = manifest.get().getInfo().getAppName();
                for (BackupManifest.Data tableData : manifest.get().getData(keyspace, table))
                {
                    Map<Boolean, Long> tableBytes =
                    tableData.getSstables().stream()
                             .flatMap(sstable -> sstable.getSstableComponents().stream())
                             .filter(component -> !BackupUtils.isMetadataFile(component.getFileName()))
                             .collect(Collectors.groupingBy(
                             BackupManifest.BackupSSTableComponent::isUploaded,
                             Collectors.summingLong(BackupManifest.BackupSSTableComponent::getFileSizeOnDisk)
                             ));
                    Long totalBytes = tableBytes.values().stream().reduce(0L, Long::sum);
                    if (totalBytes > 0)
                    {
                        result.row(tableData.getKeyspaceName(), tableData.getColumnfamilyName(), timestamp)
                              .column(APP_NAME, appName)
                              .column(TOTAL_SIZE, totalBytes)
                              .column(UPLOADED, tableBytes.getOrDefault(false, 0L) == 0L);
                    }
                }
            }
            catch (Exception e)
            {
                logger.warn("Failed to process backup metadata file: {}", key, e);
            }
        }
        return BackupUtils.toRowIterator(metadata, result, partitionKey, clusteringFilter, columnFilter);
    }
}
