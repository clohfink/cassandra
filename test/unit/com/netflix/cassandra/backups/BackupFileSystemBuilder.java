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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import static com.netflix.cassandra.backups.BackupMemtableParams.NETFLIX_ENVIRONMENT;
import static com.netflix.cassandra.backups.BackupMemtableParams.NETFLIX_REGION;
import static com.netflix.cassandra.backups.BackupMemtableParams.NETFLIX_APP;
import static com.netflix.cassandra.backups.BackupMemtableParams.TOKEN;
import static com.netflix.cassandra.backups.BackupMemtableParams.TIMESTAMP;

/**
 * Makes a fake s3 backup for the sstables in a given directory. this will expose a meta_v2.json in
 *
 * <p>ts = last_modified_time_ms<br>
 * [env]_backups/[cluster_name_hash]_cluster/[TOKEN]/META_V2/[ts]/NONE/PLAINTEXT/meta_v2_[ts].json
 *
 * <p>and all the files will be in a<br>
 * [env]_backups/[cluster_name_hash]_cluster/[TOKEN]/SST_V2/[ts]/keyspace/table-uuid/NONE/PLAINTEXT/[files]
 */
public class BackupFileSystemBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(BackupMemtableParams.class);

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Set<String> excludedFiles = Set.of(
        "manifest.json",
        "schema.cql"
    );

    private Path backupSource;
    private Path s3StoragePath;
    private Map<String, String> envVars;
    private Map<String, String> configParameters;
    private String keyspace;
    private String tableName;
    private String bucket;

    private BackupContext backupContext;
    private long backupTimestamp;
    private final String uuid = UUID.randomUUID().toString().replace("-", "");

    BackupFileSystemBuilder() {}

    BackupFileSystemBuilder setSourceBackupRootPath(String backupSource)
    {
        this.backupSource = Paths.get(backupSource);
        return this;
    }

    BackupFileSystemBuilder setEnvVars(Map<String, String> envVars)
    {
        this.envVars = envVars;
        return this;
    }

    BackupFileSystemBuilder setConfigParameters(Map<String, String> configParameters)
    {
        this.configParameters = configParameters;
        return this;
    }

    BackupFileSystemBuilder setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
        return this;
    }

    BackupFileSystemBuilder setTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public BackupFileSystemBuilder setBucket(String bucket)
    {
        this.bucket = bucket;
        return this;
    }

    public Path build() {
        // Initialize the state.
        Preconditions.checkArgument(backupSource != null, "Backup source path must be set");
        Preconditions.checkArgument(envVars != null, "Environment variables must be set");
        Preconditions.checkArgument(configParameters != null, "Config parameters must be set");
        Preconditions.checkArgument(keyspace != null, "Keyspace must be set");
        Preconditions.checkArgument(tableName != null, "Column family must be set");
        Preconditions.checkArgument(bucket != null, "Bucket must be set");

        String env = configParameters.getOrDefault(NETFLIX_ENVIRONMENT, envVars.getOrDefault(NETFLIX_ENVIRONMENT, "test"));
        String region = configParameters.getOrDefault(NETFLIX_REGION, envVars.get(NETFLIX_REGION));
        String app = configParameters.getOrDefault(NETFLIX_APP, envVars.get(NETFLIX_APP));
        String token = configParameters.getOrDefault(TOKEN, "0");
        this.backupContext = new BackupContext(env, region, app, token, bucket, null);
        this.backupTimestamp = configParameters.containsKey(TIMESTAMP)
                               ? Long.parseLong(configParameters.get(TIMESTAMP))
                               : System.currentTimeMillis();

        try
        {
            this.s3StoragePath = Files.createTempDirectory("fake-async-s3-access");
        }
        catch (IOException e)
        {
            logger.error("Failed to create temporary directory for S3 storage", e);
            throw new RuntimeException(e);
        }

        // Read all SS tables.
        List<BackupManifest.BackupSSTable> allSSTables = getAllSSTables();
        // Write SS Tables to the file system.
        try
        {
            Path ssTablesRoot = getSSTablesRoot();
            Files.createDirectories(ssTablesRoot);
            for(BackupManifest.BackupSSTable ssTable: allSSTables)
            {
                for (BackupManifest.BackupSSTableComponent component : ssTable.getSstableComponents())
                {
                    // Locally all files are stored flat. Copy to presumed destination.
                    Path s3Path = ssTablesRoot.resolve(component.getFileName());
                    Path sourcePath = backupSource.resolve(component.getFileName());
                    Files.copy(sourcePath, s3Path);
                }
            }
        }
        catch (IOException e)
        {
            logger.error("Failed to create SSTables root directory", e);
            throw new RuntimeException(e);
        }

        // Create metadata file from SSTables.
        createMetaV2(allSSTables);

        return s3StoragePath;
    }

    private void createMetaV2(List<BackupManifest.BackupSSTable> allSSTables) {
        BackupManifest backupManifest = new BackupManifest();

        BackupManifest.Info info = new BackupManifest.Info();
        info.setVersion(1);
        info.setAppName(backupContext.app());
        info.setRegion(backupContext.region());
        info.setRack(backupContext.region() + 'a');
        info.setBackupIdentifier(Lists.newArrayList(backupContext.token()));
        backupManifest.setInfo(info);

        BackupManifest.Data data = new BackupManifest.Data();
        data.setKeyspaceName(keyspace);
        data.setColumnfamilyName(tableName);
        data.setSstables(allSSTables);
        backupManifest.setData(Lists.newArrayList(data));

        Path metadataRoot = getMetadataPath();
        try
        {
            Files.createDirectories(metadataRoot.getParent());
            mapper.writeValue(metadataRoot.toFile(), backupManifest);
        }
        catch (IOException e)
        {
            logger.error("Failed to create metadata file", e);
            throw new RuntimeException(e);
        }
    }

    private Path getMetadataPath() {
        return s3StoragePath
               .resolve(bucket)
               .resolve(backupContext.prefix())
               .resolve(backupContext.token())
               .resolve("META_V2")
               .resolve(backupTimestamp + "")
               .resolve("NONE/PLAINTEXT")
               .resolve("meta_v2_" + backupTimestamp + ".json");
    }

    private Path getSSTablesRoot() {
        return s3StoragePath
               .resolve(bucket)
               .resolve(backupContext.prefix())
               .resolve(backupContext.token())
               .resolve("SST_V2")
               .resolve(backupTimestamp + "")
               .resolve(keyspace)
               .resolve(tableName + '-' + uuid)
               .resolve("NONE/PLAINTEXT");
    }

    private List<BackupManifest.BackupSSTable> getAllSSTables()
    {
        try
        {
            Path ssTablesRoot = getSSTablesRoot();
            // List all db files and convert them to S3SSTableComponent.
            List<BackupManifest.BackupSSTableComponent> allSSTableParts = Files.walk(backupSource)
                                                                               .filter(Files::isRegularFile)
                                                                               .filter(path -> !excludedFiles.contains(path.getFileName().toString()))
                                                                               .map(path -> createSSTableComponents(path, ssTablesRoot))
                                                                               .collect(Collectors.toList());
            // All components loaded, group them by id and create file structure.
            return allSSTableParts
                   .stream()
                   .collect(Collectors.groupingBy(
                        component -> component.getFileName().substring(0, component.getFileName().lastIndexOf('-'))
                   ))
                   .entrySet()
                   .stream()
                   .map(entry -> {
                       BackupManifest.BackupSSTable backupSStable = new BackupManifest.BackupSSTable();
                       backupSStable.setPrefix(entry.getKey());
                       backupSStable.setSstableComponents(entry.getValue());
                       return backupSStable;
                   })
                   .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to load SSTables", e);
        }
    }

    private BackupManifest.BackupSSTableComponent createSSTableComponents(Path filePath, Path ssTablePath)
    {
        BackupManifest.BackupSSTableComponent component = new BackupManifest.BackupSSTableComponent();
        BasicFileAttributes attributes;
        component.setFileName(filePath.getFileName().toString());
        Path relativeToBucketRoot = Paths.get(bucket).relativize(s3StoragePath.relativize(ssTablePath));
        component.setBackupPath(relativeToBucketRoot.resolve(filePath.getFileName()).toString());
        component.setCompression("NONE");
        component.setEncryption("PLAINTEXT");
        component.setUploaded(true);
        try
        {
            attributes = Files.readAttributes(filePath, BasicFileAttributes.class);
            component.setLastModifiedTime(backupTimestamp);
            component.setFileCreationTime(backupTimestamp);
            component.setFileSizeOnDisk(attributes.size());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to read file attributes for " + filePath, e);
        }
        return component;
    }
}
