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
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.cassandra.metrics.BackupMetrics;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.snapshot.TableSnapshot;
import org.apache.cassandra.utils.Clock;

/**
 * Builds the {@link BackupManifest} JSON for one Cassandra snapshot run, to be consumed by
 * the ods-java-data-tools upload tool
 * (<a href="https://github.netflix.net/corp/ods-java-data-tools">github.netflix.net/corp/ods-java-data-tools</a>,
 * see {@code com.netflix.ods.lib.conf.api.backup.Uploader}). One instance covers all
 * column families snapshotted under a single tag: the snapshot coordinator (e.g.
 * {@code StorageService.takeSnapshot}) creates a builder when {@code --netflix-manifest} is
 * in play, passes it as the {@code Consumer<TableSnapshot>} threaded through per-CF
 * snapshots, and calls {@link #write()} once everything's done.
 *
 * <p>If any per-CF snapshot throws before {@code write()} is called, the manifest is never
 * persisted and the upload tool sees nothing in {@code <datadir>/backup_manifests/} for
 * this tag — partial backups can't be uploaded.
 *
 * <p>SSTable component info comes from filesystem walks of the snapshot directories;
 * {@link Descriptor#fromFilenameWithComponent(File)} parses each component file back to its
 * {@code (Descriptor, Component)} pair. No live {@code SSTableReader}s are referenced.
 */
public final class BackupManifestBuilder implements Consumer<TableSnapshot>
{
    private static final Logger logger = LoggerFactory.getLogger(BackupManifestBuilder.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final String MANIFESTS_DIRNAME = "backup_manifests";
    /**
     * Subdirectory of {@link #MANIFESTS_DIRNAME} where freshly-written manifests land.
     * The upload tool drains from here. Sibling directories like {@code failed/} are
     * managed by the upload tool — Cassandra only ever writes to {@code pending/}.
     */
    public static final String PENDING_SUBDIR = "pending";
    public static final String COMPRESSION = "NONE";
    public static final String ENCRYPTION = "PLAINTEXT";

    private final String snapshotTag;
    private final long candidateBackupTs;
    private final BackupContext ctx;
    private final File datadir;
    private final BackupManifest.Builder manifestBuilder;
    /** Construction time, so the build-time metric covers per-CF assembly + final write. */
    private final long startNanos;

    /**
     * Returns a fresh builder for one snapshot run, or {@code null} if either the caller
     * didn't request a Netflix manifest or {@link BackupContext#isValid()} is false. The
     * builder writes to {@code <data_file_directories[0]>/../backup_manifests/pending/<snapshotTag>.json}
     * — i.e. {@code backup_manifests/} is a sibling of the Cassandra data directory, not
     * a child of it. See {@code com.netflix.ods.lib.conf.api.backup.SourceDirectories} on
     * the upload-tool side for the full layout.
     */
    public static BackupManifestBuilder tryCreate(String snapshotTag, Instant snapshotInstant, boolean enabled)
    {
        if (!enabled) return null;
        BackupContext ctx = BackupUtils.getBackupContext();
        if (!ctx.isValid())
        {
            // Misconfiguration: caller wanted a Netflix manifest but the env can't supply one.
            // Don't break the snapshot — just make sure the operator notices.
            logger.error("Skipping Netflix backup manifest for snapshot {}: BackupContext not valid", snapshotTag);
            return null;
        }
        // TODO: multi-datadir — manifests for now live next to the first data_file_directory.
        File manifestRoot = new File(DatabaseDescriptor.getAllDataFileLocations()[0]).parent();
        return new BackupManifestBuilder(snapshotTag, snapshotInstant, ctx, manifestRoot);
    }

    BackupManifestBuilder(String snapshotTag, Instant snapshotInstant, BackupContext ctx, File datadir)
    {
        this.snapshotTag = snapshotTag;
        this.candidateBackupTs = snapshotInstant.toEpochMilli();
        this.ctx = ctx;
        this.datadir = datadir;
        this.startNanos = Clock.Global.nanoTime();
        // backupPathPrefix and snapshotTag live INSIDE info so the manifest parses cleanly
        // through Priam's MetaFileReader (its top-level switch trips on unknown field names,
        // but Gson silently ignores unknown fields inside Info).
        this.manifestBuilder = BackupManifest.builder()
                .info(BackupManifest.Info.builder()
                              .version(1)
                              .appName(ctx.app())
                              .region(ctx.region())
                              .rack(availabilityZoneOrNull(ctx.region()))
                              .backupIdentifier(Collections.singletonList(ctx.token()))
                              .snapshotInstantMs(candidateBackupTs)
                              .backupPathPrefix(ctx.prefix() + '/' + ctx.token())
                              .snapshotTag(snapshotTag)
                              .build());
    }

    /** Append one CF's contribution to the in-progress manifest. */
    @Override
    public void accept(TableSnapshot snapshot)
    {
        manifestBuilder.addData(assembleData(snapshot));
    }

    /** Atomically write {@code <datadir>/backup_manifests/pending/<snapshotTag>.json}. */
    public void write() throws IOException
    {
        BackupManifest manifest = manifestBuilder.build();

        File pendingDir = new File(new File(datadir, MANIFESTS_DIRNAME), PENDING_SUBDIR);
        Files.createDirectories(pendingDir.toPath());

        File manifestFile = new File(pendingDir, snapshotTag + ".json");
        File tmpFile = new File(pendingDir, snapshotTag + ".json.tmp");
        try
        {
            mapper.writerWithDefaultPrettyPrinter()
                  .writeValue(tmpFile.toJavaIOFile(), manifest);
            Files.move(tmpFile.toPath(), manifestFile.toPath(),
                       StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        finally
        {
            // ATOMIC_MOVE leaves no tmp on success; on failure we don't want a stale .tmp lingering.
            Files.deleteIfExists(tmpFile.toPath());
        }

        BackupMetrics.backupManifestBuildTimeMs.update(TimeUnit.NANOSECONDS.toMillis(Clock.Global.nanoTime() - startNanos));
        BackupMetrics.componentsPerBackupManifest.update(countComponents(manifest));
        logger.info("Wrote Netflix backup manifest {}", manifestFile);
    }

    private BackupManifest.Data assembleData(TableSnapshot snapshot)
    {
        String keyspace = snapshot.getKeyspaceName();
        String cfName = snapshot.getTableName();
        String cfWithUuid = cfName + '-' + TableId.fromUUID(snapshot.getTableId()).toHexString();

        Map<String, BackupManifest.BackupSSTable.Builder> byPrefix = new LinkedHashMap<>();

        // TableSnapshot dedupes directories that resolve to the same physical path, so
        // getDirectories() yields each snapshot directory once even when the data dir is a symlink.
        for (File snapshotDir : snapshot.getDirectories())
        {
            File[] entries = snapshotDir.tryList();
            if (entries == null) continue;
            for (File componentFile : entries)
            {
                if (!componentFile.isFile()) continue;
                String fileName = componentFile.name();

                // SSTable components parse via Descriptor; sidecars (manifest.json,
                // schema.cql, ephemeral markers) get a synthetic prefix from the
                // filename and the candidate backup_ts — matches the Priam meta_v2
                // shape, which uploads these as components with prefix=basename.
                String prefix;
                long backupTs;
                try
                {
                    Descriptor descriptor = Descriptor.fromFilename(componentFile);
                    if (!keyspace.equals(descriptor.ksname) || !cfName.equals(descriptor.cfname))
                    {
                        // sibling files from secondary indexes happen to live in subdirectories,
                        // not this dir, so this is a defensive skip.
                        continue;
                    }
                    String generationId = descriptor.id.toString();
                    prefix = descriptor.version + "-" + generationId + "-" + descriptor.formatType.name;
                    backupTs = SystemKeyspace.getOrAssignBackupTimestamp(keyspace, cfName, generationId, candidateBackupTs);
                }
                catch (IllegalArgumentException notAnSSTable)
                {
                    int dot = fileName.lastIndexOf('.');
                    prefix = dot < 0 ? fileName : fileName.substring(0, dot);
                    backupTs = candidateBackupTs;
                }

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

                byPrefix.computeIfAbsent(prefix, p -> BackupManifest.BackupSSTable.builder().prefix(p))
                        .addSstableComponent(componentEntry);
            }
        }

        BackupManifest.Data.Builder dataBuilder =
            BackupManifest.Data.builder()
                          .keyspaceName(keyspace)
                          .columnfamilyName(cfName);
        for (BackupManifest.BackupSSTable.Builder sstableBuilder : byPrefix.values())
            dataBuilder.addSstable(sstableBuilder.build());

        return dataBuilder.build();
    }

    private static int countComponents(BackupManifest manifest)
    {
        int total = 0;
        for (BackupManifest.Data data : manifest.getData())
            for (BackupManifest.BackupSSTable sstable : data.getSstables())
                total += sstable.getSstableComponents().size();
        return total;
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
     * Reconstruct the full AWS availability zone (e.g. "us-east-1a") from the AWS region
     * and the snitch's local rack. The Netflix snitch uses legacy naming where the rack
     * is just the last "<digit><letter>" segment of the AZ; {@link #composeAz} combines
     * it with the region (minus its trailing digits) to recover the original AZ.
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
