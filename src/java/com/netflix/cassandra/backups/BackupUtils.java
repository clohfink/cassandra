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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;
import software.amazon.awssdk.regions.Region;

/**
 * Utility class containing common backup-related functionality shared across
 * BackupMemtableParams, BackupMemtable, and backup virtual tables.
 */
public class BackupUtils
{
    private static final Logger logger = LoggerFactory.getLogger(BackupUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String NETFLIX_REGION = "NETFLIX_REGION";
    public static final String NETFLIX_APP = "NETFLIX_APP";
    public static final String NETFLIX_ENVIRONMENT = "NETFLIX_ENVIRONMENT";

    public static BackupContext getBackupContext()
    {
        Map<String, String> env = System.getenv();
        return new BackupContext(env.get(NETFLIX_ENVIRONMENT), env.get(NETFLIX_REGION), env.get(NETFLIX_APP), getToken());
    }

    public static ObjectStoreAccess getObjectStoreAccess()
    {
        ObjectStoreConfiguration config = ObjectStoreConfiguration.defaultRetryConfig;
        Region region;
        try
        {
            region = Region.of(System.getenv(NETFLIX_REGION));
        }
        catch (Exception e)
        {
            return null;
        }
        return ObjectStoreAccess.get(region, config);
    }

    /**
     * Extracts timestamp from an S3 key path.
     * Expected format: .../timestamp/keyspace/table/...
     * The timestamp is the 4th segment from the end.
     *
     * @param key The S3 key path
     * @return The timestamp in milliseconds, or -1 if parsing fails
     */
    public static long extractTimestampFromKey(String key)
    {
        String[] segments = key.split("/");
        if (segments.length < 4)
            return -1;

        try
        {
            return Long.parseLong(segments[segments.length - 4]);
        }
        catch (NumberFormatException e)
        {
            return -1;
        }
    }

    /**
     * Checks if a component file name is a metadata file that should be excluded from
     * SSTable component processing. Metadata files (manifest.json, schema.cql) don't
     * represent actual SSTable data.
     *
     * @param fileName The component file name
     * @return true if this is a metadata file
     */
    public static boolean isMetadataFile(String fileName)
    {
        return "manifest.json".equals(fileName) || "schema.cql".equals(fileName);
    }

    /**
     * Gets the current node's token from StorageService, falling back to DatabaseDescriptor.
     *
     * @return The token string, or null if not available
     */
    public static String getToken()
    {
        try
        {
            return StorageService.instance.getTokens().get(0);
        }
        catch (Throwable t)
        {
            Iterator<String> iterator = DatabaseDescriptor.getInitialTokens().iterator();
            if (iterator.hasNext())
            {
                return iterator.next();
            }
            return null;
        }
    }

    /**
     * Converts SimpleDataSet partition to UnfilteredRowIterator.
     *
     * @param metadata TableMetadta referencing the virtual table
     */
    public static UnfilteredRowIterator toRowIterator(TableMetadata metadata, SimpleDataSet dataSet, DecoratedKey partitionKey,
                                                      ClusteringIndexFilter clusteringFilter, ColumnFilter columnFilter)
    {
        AbstractVirtualTable.Partition partition = dataSet.getPartition(partitionKey);
        if (partition == null)
        {
            // No matching partition - return empty iterator
            return EmptyIterators.unfilteredRow(metadata, partitionKey, false);
        }

        // Use provided filters, or default to all if not provided
        ClusteringIndexFilter filter = clusteringFilter != null ? clusteringFilter : new ClusteringIndexSliceFilter(Slices.ALL, false);
        ColumnFilter colFilter = columnFilter != null ? columnFilter : ColumnFilter.all(metadata);

        return partition.toRowIterator(metadata, filter, colFilter, Clock.Global.nanoTime());
    }

    public static Optional<BackupManifest> getManifest(Future<byte[]> future)
    {
        try
        {
            return Optional.of(mapper.readValue(future.get(), BackupManifest.class));
        }
        catch (Exception e)
        {
            return Optional.empty();
        }
    }

    /**
     * Parses a BackupManifest from an InputStream.
     *
     * @param in The input stream containing the manifest JSON
     * @return The parsed BackupManifest
     * @throws IOException if parsing fails
     */
    public static BackupManifest getManifest(InputStream in) throws IOException
    {
        return mapper.readValue(in, BackupManifest.class);
    }
}
