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
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.cassandra.metrics.BackupMetrics;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;

/**
 * Builds the {@link BackupManifest} JSON written alongside each Cassandra snapshot,
 * to be consumed by Priam (Netflix's Cassandra backup sidecar) when uploading to S3.
 *
 * <p>Priam lays each node's data out in S3 under:
 * <pre>
 *   s3://{region}-cass-{env}-1/{env}_backup/{hashCode(app) % 10000}_{app}/{token}/
 *       META_V2/{ts}/{NONE|SNAPPY}/PLAINTEXT/meta_v2_{YYYYMMDDHHMM}.json
 *       SST_V2/{ts}/{keyspace}/{cf}-{uuid}/{NONE|SNAPPY}/PLAINTEXT/{component-file}
 *       SECONDARY_INDEX_V2/{ts}/{keyspace}/{cf}-{uuid}/.{idx}_idx/{NONE|SNAPPY}/PLAINTEXT/{component-file}
 * </pre>
 * {@code PLAINTEXT} is a legacy encryption marker from when backups were dual-written
 * to GCS (the alternative {@code PGP} is deprecated). SSTable objects are immutable
 * across backups, so a single meta_v2.json may reference components scattered across
 * multiple timestamp prefixes. The per-CF manifest produced here mirrors the SST_V2
 * layout for the components captured in this snapshot.
 *
 * @see <a href="https://docs.google.com/document/d/1eqnEr4DUW3CWVQrBO6umWesWwxU2N_uqM84XjKieNeY/edit?tab=t.0#heading=h.4uv0cwuufztl">Netflix Cassandra Backup Structure</a>
 */
public class BackupManifestBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(BackupManifestBuilder.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String MANIFEST_FILENAME = "backup_manifest.json";
    public static final String COMPRESSION = "NONE";
    public static final String ENCRYPTION = "PLAINTEXT";

    /**
     * Builds a {@link BackupManifest} for the given set of snapshotted SSTables
     * and writes it to {@code snapshotDir/backup_manifest.json}.
     *
     * @param sstables        readers that were included in the snapshot
     * @param ctx             backup context (env, region, app, token)
     * @param snapshotInstant the snapshot creation instant; the millis value
     *                        is the candidate backup_timestamp for any newly
     *                        tracked SSTable
     * @param metadata        table metadata; id is used to build the {@code cf-uuid}
     *                        folder segment
     * @param snapshotDir     the snapshot directory; the manifest is written here
     */
    public static void build(Collection<SSTableReader> sstables,
                             BackupContext ctx,
                             Instant snapshotInstant,
                             TableMetadata metadata,
                             File snapshotDir) throws IOException
    {
        long startNanos = Clock.Global.nanoTime();
        BackupManifest manifest = assemble(sstables, ctx, snapshotInstant, metadata, snapshotDir);
        File manifestFile = new File(snapshotDir, MANIFEST_FILENAME);
        mapper.writerWithDefaultPrettyPrinter()
              .writeValue(manifestFile.toJavaIOFile(), manifest);
        BackupMetrics.backupManifestBuildTimeMs.update(TimeUnit.NANOSECONDS.toMillis(Clock.Global.nanoTime() - startNanos));
        BackupMetrics.componentsPerBackupManifest.update(countComponents(manifest));
        logger.debug("Wrote Netflix backup manifest {}", manifestFile);
    }

    private static int countComponents(BackupManifest manifest)
    {
        int total = 0;
        for (BackupManifest.Data data : manifest.getData())
            for (BackupManifest.BackupSSTable sstable : data.getSstables())
                total += sstable.getSstableComponents().size();
        return total;
    }

    static BackupManifest assemble(Collection<SSTableReader> sstables,
                                   BackupContext ctx,
                                   Instant snapshotInstant,
                                   TableMetadata metadata,
                                   File snapshotDir)
    {
        String keyspace = metadata.keyspace;
        String cfWithUuid = metadata.name + '-' + metadata.id.toHexString();
        long candidateBackupTs = snapshotInstant.toEpochMilli();

        Map<String, BackupManifest.BackupSSTable.Builder> byPrefix = new LinkedHashMap<>();

        for (SSTableReader sstable : sstables)
        {
            if (!metadata.keyspace.equals(sstable.descriptor.ksname)
                || !metadata.name.equals(sstable.descriptor.cfname))
            {
                // snapshot can include sibling CFS (indexes); manifest is per-CF
                continue;
            }

            String generationId = sstable.descriptor.id.toString();
            String prefix = sstable.descriptor.version
                            + "-" + generationId
                            + "-" + sstable.descriptor.formatType.name;

            long backupTs = SystemKeyspace.getOrAssignBackupTimestamp(keyspace,
                                                                     metadata.name,
                                                                     generationId,
                                                                     candidateBackupTs);

            BackupManifest.BackupSSTable.Builder sstableBuilder =
                byPrefix.computeIfAbsent(prefix,
                                         p -> BackupManifest.BackupSSTable.builder().prefix(p));

            for (Component component : sstable.getComponents())
            {
                String fileName = sstable.descriptor.version
                                  + "-" + generationId
                                  + "-" + sstable.descriptor.formatType.name
                                  + "-" + component.name();
                File componentFile = new File(snapshotDir, fileName);
                if (!componentFile.exists())
                    continue;

                String backupPath = ctx.sstableComponentPath(backupTs,
                                                             keyspace,
                                                             cfWithUuid,
                                                             COMPRESSION,
                                                             ENCRYPTION,
                                                             fileName);

                long size = componentFile.length();
                long lastModified = componentFile.lastModified();
                long creationTime = readCreationTime(componentFile, lastModified);

                BackupManifest.BackupSSTableComponent componentEntry =
                    BackupManifest.BackupSSTableComponent.builder()
                                  .fileName(fileName)
                                  .fileSizeOnDisk(size)
                                  .lastModifiedTime(lastModified)
                                  .fileCreationTime(creationTime)
                                  .compression(COMPRESSION)
                                  .encryption(ENCRYPTION)
                                  .isUploaded(false)
                                  .backupPath(backupPath)
                                  .build();
                sstableBuilder.addSstableComponent(componentEntry);
            }
        }

        BackupManifest.Data.Builder dataBuilder =
            BackupManifest.Data.builder()
                          .keyspaceName(keyspace)
                          .columnfamilyName(metadata.name);
        for (BackupManifest.BackupSSTable.Builder sstableBuilder : byPrefix.values())
            dataBuilder.addSstable(sstableBuilder.build());

        BackupManifest.Info info =
            BackupManifest.Info.builder()
                          .version(1)
                          .appName(ctx.app())
                          .region(ctx.region())
                          .rack(availabilityZoneOrNull(ctx.region()))
                          .backupIdentifier(Collections.singletonList(ctx.token()))
                          .build();

        return BackupManifest.builder()
                             .info(info)
                             .addData(dataBuilder.build())
                             .build();
    }

    private static long readCreationTime(File file, long fallbackMillis)
    {
        try
        {
            BasicFileAttributes attrs = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            return attrs.creationTime().toMillis();
        }
        catch (IOException e)
        {
            return fallbackMillis;
        }
    }

    /**
     * Reconstruct the full AWS availability zone (e.g. "us-east-1a") from the
     * AWS region and the snitch's local rack. The Netflix snitch uses legacy
     * naming where the rack is just the last "<digit><letter>" segment of the
     * AZ; {@link #composeAz} combines it with the region (minus its trailing
     * digits) to recover the original AZ.
     */
    static String availabilityZoneOrNull(String region)
    {
        String rack;
        try
        {
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            rack = snitch == null ? null : snitch.getLocalRack();
        }
        catch (Throwable t)
        {
            return null;
        }
        return composeAz(region, rack);
    }

    static String composeAz(String region, String rack)
    {
        if (rack == null || rack.isEmpty())
            return null;
        if (region == null || region.isEmpty())
            return rack;
        return region.replaceAll("\\d+$", "") + rack;
    }
}
