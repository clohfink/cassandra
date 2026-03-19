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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.metrics.ColdTierMetrics;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.sstable.format.big.BigTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.apache.cassandra.io.sstable.SSTable.componentsFor;
import static org.apache.cassandra.io.sstable.format.SSTableReader.OpenReason.NORMAL;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Holds state and handles the initialization of a BackupMemtable: downloading the manifest,
 * fetching SSTable components from S3, and building SSTableReaders.
 */
class BackupMemtableContext implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(BackupMemtableContext.class);
    private static final String TIMESTAMP_MARKER = ".timestamp";

    static final Component[] COMPONENTS_TO_DOWNLOAD = {
        Component.FILTER,
        Component.TOC,
        Component.SUMMARY,
        Component.STATS,
        Component.PRIMARY_INDEX,
        Component.COMPRESSION_INFO
    };

    private final BackupMemtableParams params;
    private final TableMetadataRef metadataRef;

    // Results populated by initialize()
    private final List<BackupDescriptor> descriptors = new ArrayList<>();
    private final List<SSTableReader> sstables = new ArrayList<>();
    private SSTableIntervalTree intervalTree;

    BackupMemtableContext(BackupMemtableParams params, TableMetadataRef metadataRef)
    {
        this.params = params;
        this.metadataRef = metadataRef;
    }

    @Override
    public void run()
    {
        long startMs = currentTimeMillis();
        try
        {
            initialize();
        }
        catch (ExecutionException | InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            ColdTierMetrics.backupMemtableInitTimeMs.update(currentTimeMillis() - startMs);
        }
    }

    private void initialize() throws ExecutionException, InterruptedException
    {
        // Clear state so this method is safe to re-run on retry
        descriptors.clear();
        sstables.clear();
        intervalTree = null;

        // Prepare cache directory. If the timestamp changed (e.g. ALTER TABLE with new timestamp),
        // delete only the meta file and marker — NOT the component files, since the old memtable
        // may still be serving reads from its SSTableReaders in those files.
        // The new timestamp's manifest references different SSTables with different file names,
        // so new components are downloaded alongside the old ones without conflict.
        File cache = BackupDescriptor.prepareCacheDir(metadataRef);
        if (isCacheStale(cache))
        {
            logger.info("Stale cache detected for timestamp {}, re-downloading manifest", params.getTimestamp());
            new File(cache, "meta_v2.json").toJavaIOFile().delete();
            new File(cache, TIMESTAMP_MARKER).toJavaIOFile().delete();
        }

        // Download and load manifest
        File meta = new File(cache, "meta_v2.json");
        if (!meta.exists())
            downloadClosestMeta(meta);
        else
            logger.info("Using cached meta file: {}", meta);

        BackupManifest fullManifest = params.loadManifest(meta);
        String ks = params.getEffectiveKeyspace(metadataRef);
        String tbl = params.getEffectiveTable(metadataRef);
        BackupManifest.Data manifest = params.findTableData(fullManifest, ks, tbl);

        List<BackupDescriptor> allDescriptors = collectDescriptors(manifest);
        List<AsyncPromise<?>> downloads = downloadRequiredComponents(allDescriptors);
        waitForDownloads(downloads);
        filterValidDescriptors(allDescriptors);
        intervalTree = SSTableIntervalTree.build(sstables);

        // Mark cache as valid for this timestamp
        writeTimestampMarker(cache);
    }

    List<BackupDescriptor> getDescriptors()
    {
        return descriptors;
    }

    List<SSTableReader> getSstables()
    {
        return sstables;
    }

    SSTableIntervalTree getIntervalTree()
    {
        return intervalTree;
    }

    /**
     * Returns true if the cache directory contains data for a different timestamp than the current params.
     * A missing marker (first run or failed previous run) is not considered stale.
     */
    private boolean isCacheStale(File cacheDir)
    {
        File marker = new File(cacheDir, TIMESTAMP_MARKER);
        if (!marker.exists())
            return false;
        try
        {
            long cachedTs = Long.parseLong(
                new String(java.nio.file.Files.readAllBytes(marker.toPath())).trim());
            return cachedTs != params.getTimestamp();
        }
        catch (Exception e)
        {
            logger.warn("Failed to read timestamp marker, treating cache as stale", e);
            return true;
        }
    }

    private void writeTimestampMarker(File cacheDir)
    {
        File marker = new File(cacheDir, TIMESTAMP_MARKER);
        try
        {
            java.nio.file.Files.write(marker.toPath(),
                                      Long.toString(params.getTimestamp()).getBytes());
        }
        catch (IOException e)
        {
            logger.warn("Failed to write timestamp marker", e);
        }
    }

    void downloadClosestMeta(File metaFile) throws ExecutionException, InterruptedException
    {
        ObjectStoreAccess client = params.getS3();
        String prefixPath = params.getMetafilePrefix();
        List<String> keys = client.getObjectKeys(params.getBucket(), prefixPath).get();

        long   bestTs = 0;
        String bestKey = null;
        long   timestamp = params.getTimestamp();
        for (String key : keys)
        {
            long ts = BackupUtils.extractTimestampFromKey(key);
            if (ts >= 0 && ts <= timestamp && (bestTs == 0 || ts > bestTs))
            {
                bestTs = ts;
                bestKey = key;
            }
        }

        if (bestKey == null)
            throw new RuntimeException("No suitable meta_v2.json found at path " +
                                       prefixPath + " for timestamp " + timestamp);

        long ageMs = timestamp - bestTs;
        logger.info("Selected meta file: {} (ts={}, age={}ms / {}h)", bestKey, bestTs, ageMs,
                     TimeUnit.MILLISECONDS.toHours(ageMs));

        int maxRetries = 3;
        int timeoutMs = DatabaseDescriptor.getObjectStoreFetchTimeoutMs();
        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                client.getObjectAsFile(params.getBucket(), bestKey, metaFile.toPath())
                      .get(timeoutMs, TimeUnit.MILLISECONDS);
                return;
            }
            catch (TimeoutException e)
            {
                logger.warn("Timeout downloading meta file {} (attempt {}/{})", bestKey, attempt, maxRetries);
                if (attempt == maxRetries)
                    throw new RuntimeException("Timed out downloading meta file " + bestKey + " after " + maxRetries + " attempts", e);
            }
            catch (ExecutionException e)
            {
                logger.warn("Failed to download meta file {} (attempt {}/{}): {}", bestKey, attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries)
                    throw new RuntimeException("Failed to download meta file " + bestKey + " after " + maxRetries + " attempts", e);
            }
        }
    }

    private List<BackupDescriptor> collectDescriptors(BackupManifest.Data manifest)
    {
        List<BackupDescriptor> allDescriptors = new ArrayList<>();

        for (BackupManifest.BackupSSTable sstable : manifest.getSstables())
        {
            try
            {
                if (sstable.getPrefix().equals("manifest") || sstable.getPrefix().equals("schema"))
                    continue;
                allDescriptors.add(new BackupDescriptor(sstable, metadataRef, params.getBucket()));
            }
            catch (IllegalArgumentException e)
            {
                // If we can't parse the descriptor, skip it
                logger.debug("Skipping invalid backup descriptor: {}", sstable.getSstableComponents(), e);
            }
        }

        return allDescriptors;
    }

    private List<AsyncPromise<?>> downloadRequiredComponents(List<BackupDescriptor> allDescriptors)
    {
        List<AsyncPromise<?>> downloads = new ArrayList<>();

        for (BackupDescriptor desc : allDescriptors)
        {
            // Download required component files
            for (Component comp : COMPONENTS_TO_DOWNLOAD)
            {
                File local = new File(desc.filenameFor(comp));
                if (!local.exists())
                {
                    String key = desc.s3KeyFor(comp);
                    AsyncPromise<Void> fileDownload = params.getS3().getObjectAsFile(params.getBucket(), key, local.toPath());
                    downloads.add(fileDownload);
                }
            }

            // Fetch and store Data file length
            File dataLenFile = new File(desc.filenameFor(Component.DATA) + ".len");
            if (!dataLenFile.exists())
            {
                String dataKey = desc.s3KeyFor(Component.DATA);
                AsyncPromise<Void> writeLenPromise = new AsyncPromise<>();
                params.getS3().getObjectSize(desc.bucket, dataKey)
                    .addCallback(
                        length -> {
                            try
                            {
                                DataLengthFileSerializer.write(dataLenFile, length);
                                writeLenPromise.setSuccess(null);
                            }
                            catch (IOException e)
                            {
                                writeLenPromise.setFailure(e);
                            }
                        },
                        writeLenPromise::setFailure
                    );
                downloads.add(writeLenPromise);
            }
        }
        return downloads;
    }

    private void waitForDownloads(List<AsyncPromise<?>> downloads)
    {
        int timeoutMs = DatabaseDescriptor.getObjectStoreFetchTimeoutMs();
        for (AsyncPromise<?> download : downloads)
        {
            try
            {
                download.get(timeoutMs, TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException e)
            {
                logger.warn("Timed out waiting for component download after {}ms", timeoutMs);
            }
            catch (CompletionException e)
            {
                if (e.getCause() instanceof S3Exception)
                {
                    S3Exception s3Exception = (S3Exception) e.getCause();
                    logger.debug("Failed to download components from S3: " + s3Exception.getMessage());
                }
            }
            catch (ExecutionException e)
            {
                if (e.getCause() instanceof S3Exception)
                {
                    logger.debug("Failed to download components from S3: " + e.getCause().getMessage());
                }
                else
                {
                    logger.warn("Failed to download component from S3", e.getCause());
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting for component downloads", e);
            }
        }
    }

    private void filterValidDescriptors(List<BackupDescriptor> allDescriptors)
    {
        for (BackupDescriptor desc : allDescriptors)
        {
            boolean allPresent = Arrays.stream(COMPONENTS_TO_DOWNLOAD)
                .allMatch(comp -> new File(desc.filenameFor(comp)).exists());
            if (allPresent)
            {
                descriptors.add(desc);
                try
                {
                    EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
                    Map<MetadataType, MetadataComponent> sstableMetadata;
                    try
                    {
                        sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
                    }
                    catch (IOException e)
                    {
                        throw new CorruptSSTableException(e, desc.filenameFor(Component.STATS));
                    }

                    StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
                    SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);

                    SSTableReader sstable = new ForS3(params.getS3(), desc, metadataRef, componentsFor(desc), statsMetadata, header.toHeader(metadataRef.get())).build();
                    this.sstables.add(sstable);
                }
                catch (ExecutionException | InterruptedException | IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Serializes and deserializes the compressed Data file length to a local cache file.
     * <p>
     * Binary format (14 bytes, fixed size):
     * <pre>
     *   [0-1]   'D' 'L'                  magic bytes "Data Length"
     *   [2-9]   8-byte big-endian long   data file length
     *   [10-13] 4-byte big-endian int    CRC32 over bytes [2-9]
     * </pre>
     */
    static class DataLengthFileSerializer
    {
        static final byte[] MAGIC = { 'D', 'L' };
        static final int FILE_SIZE = MAGIC.length + Long.BYTES + Integer.BYTES;

        static void write(File file, long length) throws IOException
        {
            byte[] buf = new byte[FILE_SIZE];
            buf[0] = MAGIC[0];
            buf[1] = MAGIC[1];
            for (int i = 0; i < Long.BYTES; i++)
                buf[MAGIC.length + i] = (byte) (length >>> (56 - i * 8));

            CRC32 crc = new CRC32();
            crc.update(buf, MAGIC.length, Long.BYTES);
            int crcVal = (int) crc.getValue();
            int off = MAGIC.length + Long.BYTES;
            for (int i = 0; i < Integer.BYTES; i++)
                buf[off + i] = (byte) (crcVal >>> (24 - i * 8));

            java.nio.file.Files.write(file.toPath(), buf);
        }

        static long read(String path) throws IOException
        {
            byte[] raw = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path));

            if (raw.length != FILE_SIZE || raw[0] != MAGIC[0] || raw[1] != MAGIC[1])
                throw new IOException("Invalid length file (expected 14-byte DL format): " + path);

            CRC32 crc = new CRC32();
            crc.update(raw, MAGIC.length, Long.BYTES);
            int expected = 0;
            int off = MAGIC.length + Long.BYTES;
            for (int i = 0; i < Integer.BYTES; i++)
                expected = (expected << 8) | (raw[off + i] & 0xFF);

            if ((int) crc.getValue() != expected)
                throw new IOException("Checksum mismatch in length file: " + path);

            long length = 0;
            for (int i = 0; i < Long.BYTES; i++)
                length = (length << 8) | (raw[MAGIC.length + i] & 0xFF);
            return length;
        }
    }

    public static class ForS3 extends SSTableReaderBuilder
    {
        BackupDescriptor desc;
        ObjectStoreAccess access;
        public ForS3(ObjectStoreAccess access,
                     BackupDescriptor descriptor,
                     TableMetadataRef metadataRef,
                     Set<Component> components,
                     StatsMetadata statsMetadata,
                     SerializationHeader header) throws ExecutionException, InterruptedException, IOException
        {
            super(descriptor, metadataRef, currentTimeMillis(), components, statsMetadata, NORMAL, header);
            this.desc = descriptor;
            this.access = access;

            this.loadSummary();
            this.bf = this.loadBloomFilter();
            long compressedDataLength = DataLengthFileSerializer.read(desc.filenameFor(Component.DATA) + ".len");

            // Create and immediately close the builder after getting the FileHandle
            FileHandle.Builder ifileBuilder = new FileHandle.Builder(descriptor.filenameFor(Component.PRIMARY_INDEX));
            this.ifile = ifileBuilder.complete();
            ifileBuilder.close();

            // Get the actual S3 compressed data file size
            CompressionMetadata compressionMetadata = CompressionMetadata.createWithLength(desc.filenameFor(Component.COMPRESSION_INFO), compressedDataLength);
            this.dfile = new BackupFileHandle(access, desc.bucket, desc.s3KeyFor(Component.DATA), compressionMetadata, compressionMetadata.dataLength);
        }

        @Override
        public SSTableReader build()
        {
            SSTableReader sstable = new BigTableReader(this);
            sstable.setup(false);
            sstable.first = first;
            sstable.last = last;
            return sstable;
        }
    }
}
