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
import java.nio.file.Files;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadataRef;

public class BackupDescriptor extends Descriptor
{
    public final BackupManifest.BackupSSTable sstable;
    public String bucket;

    public BackupDescriptor(BackupManifest.BackupSSTable sstable, TableMetadataRef metadataRef, String bucket)
    {
        // we pass a dummy File directory into the super ctor because we override baseFilename()
        super(getVersion(sstable),
              new File("."),
              getKeyspaceName(sstable),
              getTableName(sstable),
              getId(sstable),
              getType(sstable));
        this.sstable = sstable;
        this.directory = prepareCacheDir(metadataRef);
        this.bucket = bucket;
    }

    static SSTableFormat.Type getType(BackupManifest.BackupSSTable sstable)
    {
        // prefix format is: version-seq-formatType
        String[] parts = sstable.getPrefix().split("-");
        return SSTableFormat.Type.valueOf(parts[2].toUpperCase());
    }

    static SSTableId getId(BackupManifest.BackupSSTable sstable)
    {
        // prefix format is: version-seq-formatType
        String[] parts = sstable.getPrefix().split("-");
        return new SequenceBasedSSTableId.Builder().fromString(parts[1]);
    }

    static String getTableName(BackupManifest.BackupSSTable sstable)
    {
        String[] parts = sstable.getSstableComponents().get(0).getBackupPath().split("/");
        // Format: prefix/hash_cluster/token/SST_V2/timestamp/keyspace/cfWithUuid/compression/encryption/file
        return parts[6].split("-")[0];
    }

    static String getKeyspaceName(BackupManifest.BackupSSTable manifest)
    {
        String[] parts = manifest.getSstableComponents().get(0).getBackupPath().split("/");
        // Format: prefix/hash_cluster/token/SST_V2/timestamp/keyspace/cfWithUuid/compression/encryption/file
        return parts[5];
    }

    static String getVersion(BackupManifest.BackupSSTable sstable)
    {
        return sstable.getPrefix().split("-")[0];
    }

    @Override
    public String baseFilename()
    {
        return directory.path()
               + '/'
               + version
               + Component.separator
               + id
               + Component.separator
               + formatType.name;
    }

    @Override
    public String filenameFor(Component component)
    {
        return baseFilename() + Component.separator + component.name();
    }

    public String s3KeyFor(Component component)
    {
        for (BackupManifest.BackupSSTableComponent c : this.sstable.getSstableComponents())
        {
            if (c.getFileName().endsWith(component.name()))
            {
                return c.getBackupPath();
            }
        }
        throw new RuntimeException("No s3 key found for " + component.name());
    }

    /**
     * Prepares the temp directory for s3 in the given table's directory.
     *
     * @param meta Reference to the table metadata
     * @return The cache directory File object
     * @throws IllegalStateException if JBOD is configured
     * @throws RuntimeException if the cache directory cannot be created
     */
    public static File prepareCacheDir(TableMetadataRef meta)
    {
        Directories dirs = new Directories(meta.get());
        if (dirs.getCFDirectories().size() != 1)
            throw new IllegalStateException("JBOD not supported");

        File base = dirs.getCFDirectories().get(0);
        File cache = new File(base, "s3");
        if (!cache.exists())
        {
            try
            {
                Files.createDirectories(cache.toPath());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        return cache;
    }
}
