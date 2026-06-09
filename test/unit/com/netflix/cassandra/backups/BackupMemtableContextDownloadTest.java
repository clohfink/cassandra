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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for component download validation: size checks against manifest,
 * atomic downloads, truncated file detection, and retry after validation failure.
 */
public class BackupMemtableContextDownloadTest
{
    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "test/prefix";
    private static final String TOKEN = "3";
    private static final String BACKUP_PATH_PREFIX = "backup/hash/3/SST_V2/1000/testks/testtable-abc123/NONE/PLAINTEXT/";

    private AwsAsyncS3FakeBackup fakeS3;
    private Path tempDir;
    private Path cacheDir;
    private Path s3Root;
    private BackupMemtableContext ctx;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup() throws IOException
    {
        tempDir = Files.createTempDirectory("download-test");
        cacheDir = tempDir.resolve("cache");
        Files.createDirectories(cacheDir);
        s3Root = tempDir.resolve("s3root");
        Files.createDirectories(s3Root);

        Map<String, String> envVars = new HashMap<>();
        envVars.put("NETFLIX_REGION", "us-east-1");
        envVars.put("NETFLIX_APP", "testapp");
        envVars.put("NETFLIX_ENVIRONMENT", "test");

        fakeS3 = new AwsAsyncS3FakeBackup(envVars, Region.US_EAST_1);
        fakeS3.setFakeS3RootDir(s3Root.toString());
        ObjectStoreAccess.set(Region.US_EAST_1, fakeS3);

        String config = String.format(
            "backupmemtable:prefix=%s,bucket=%s,keyspace=testks,token=%s,table=testtable,timestamp=1000",
            PREFIX, BUCKET, TOKEN);
        BackupMemtableParams params = new BackupMemtableParams(config, envVars);
        ctx = new BackupMemtableContext(params, null);
    }

    @After
    public void tearDown() throws IOException
    {
        Files.walk(tempDir)
             .sorted((a, b) -> b.compareTo(a))
             .forEach(path -> {
                 try { Files.delete(path); }
                 catch (IOException ignored) {}
             });
    }

    /**
     * Builds a BackupSSTable with components of specified sizes.
     * Each component gets a backupPath that the fake S3 can resolve.
     * Always includes a Data.db component (needed by downloadRequiredComponents for length file).
     */
    private BackupManifest.BackupSSTable buildSSTable(String prefix, Map<Component, Long> componentSizes)
    {
        BackupManifest.BackupSSTable.Builder builder = BackupManifest.BackupSSTable.builder().prefix(prefix);
        for (Map.Entry<Component, Long> entry : componentSizes.entrySet())
        {
            String fileName = prefix + "-" + entry.getKey().name();
            builder.addSstableComponent(
                BackupManifest.BackupSSTableComponent.builder()
                    .fileName(fileName)
                    .fileSizeOnDisk(entry.getValue())
                    .backupPath(BACKUP_PATH_PREFIX + fileName)
                    .compression("NONE")
                    .encryption("PLAINTEXT")
                    .isUploaded(true)
                    .build());
        }
        // Always include Data.db — downloadRequiredComponents needs it for the .len file
        if (!componentSizes.containsKey(Component.DATA))
        {
            String dataFileName = prefix + "-" + Component.DATA.name();
            builder.addSstableComponent(
                BackupManifest.BackupSSTableComponent.builder()
                    .fileName(dataFileName)
                    .fileSizeOnDisk(1024)
                    .backupPath(BACKUP_PATH_PREFIX + dataFileName)
                    .compression("NONE")
                    .encryption("PLAINTEXT")
                    .isUploaded(true)
                    .build());
        }
        return builder.build();
    }

    private Map<Component, Long> allComponentSizes(long size)
    {
        Map<Component, Long> sizes = new HashMap<>();
        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            sizes.put(comp, size);
        return sizes;
    }

    private BackupDescriptor makeDescriptor(BackupManifest.BackupSSTable sstable)
    {
        return new BackupDescriptor(sstable, new File(cacheDir.toString()), BUCKET);
    }

    /**
     * Creates a file in the fake S3 directory with the given content size.
     */
    private void createS3File(BackupDescriptor desc, Component comp, int size) throws IOException
    {
        String key = desc.s3KeyFor(comp);
        Path s3File = s3Root.resolve(BUCKET).resolve(key);
        Files.createDirectories(s3File.getParent());
        Files.write(s3File, new byte[size]);
    }

    /**
     * Ensures the Data.db file exists in fake S3 so downloadRequiredComponents
     * can resolve its size for the .len file.
     */
    private void ensureDataFileInS3(BackupDescriptor desc) throws IOException
    {
        createS3File(desc, Component.DATA, 1024);
    }

    /**
     * Creates a local cached file with the given content size.
     */
    private void createCachedFile(BackupDescriptor desc, Component comp, int size) throws IOException
    {
        Path local = Path.of(desc.filenameFor(comp));
        Files.createDirectories(local.getParent());
        Files.write(local, new byte[size]);
    }

    // ---- validateCachedComponents tests ----

    @Test
    public void testValidate_CorrectSizeFileIsKept() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(100));
        BackupDescriptor desc = makeDescriptor(sstable);
        createCachedFile(desc, Component.PRIMARY_INDEX, 100);

        ctx.validateCachedComponents(Collections.singletonList(desc));

        assertTrue("Correctly sized file should be kept",
                   new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
    }

    @Test
    public void testValidate_TruncatedFileIsDeleted() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(1000));
        BackupDescriptor desc = makeDescriptor(sstable);
        // Create a file that's smaller than the manifest says
        createCachedFile(desc, Component.PRIMARY_INDEX, 500);

        ctx.validateCachedComponents(Collections.singletonList(desc));

        assertFalse("Truncated file should be deleted",
                    new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
    }

    @Test
    public void testValidate_OversizedFileIsDeleted() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(100));
        BackupDescriptor desc = makeDescriptor(sstable);
        createCachedFile(desc, Component.PRIMARY_INDEX, 200);

        ctx.validateCachedComponents(Collections.singletonList(desc));

        assertFalse("Oversized file should be deleted",
                    new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
    }

    @Test
    public void testValidate_MissingFileIsIgnored() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(100));
        BackupDescriptor desc = makeDescriptor(sstable);
        // Don't create any cached file — should not throw
        ctx.validateCachedComponents(Collections.singletonList(desc));
    }

    @Test
    public void testValidate_MultipleComponents_OnlyBadOnesDeleted() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(100));
        BackupDescriptor desc = makeDescriptor(sstable);

        // Index.db is correct size, Filter.db is truncated
        createCachedFile(desc, Component.PRIMARY_INDEX, 100);
        createCachedFile(desc, Component.FILTER, 50);

        ctx.validateCachedComponents(Collections.singletonList(desc));

        assertTrue("Correctly sized Index.db should be kept",
                   new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
        assertFalse("Truncated Filter.db should be deleted",
                    new File(desc.filenameFor(Component.FILTER)).exists());
    }

    // ---- downloadRequiredComponents tests ----

    @Test
    public void testDownload_MissingFileIsDownloaded() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(64));
        BackupDescriptor desc = makeDescriptor(sstable);

        // Put files in fake S3
        createS3File(desc, Component.PRIMARY_INDEX, 64);
        ensureDataFileInS3(desc);

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        assertTrue("Downloaded file should exist",
                   new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
        assertEquals("Downloaded file should have correct size",
                     64, new File(desc.filenameFor(Component.PRIMARY_INDEX)).length());
    }

    @Test
    public void testDownload_ExistingFileIsNotRedownloaded() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(64));
        BackupDescriptor desc = makeDescriptor(sstable);

        // Pre-create all cached files and the data len file so nothing needs downloading
        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            createCachedFile(desc, comp, 64);
        ensureDataFileInS3(desc);
        // Pre-create the .len file too
        BackupMemtableContext.DataLengthFileSerializer.write(
            new File(desc.filenameFor(Component.DATA) + ".len"), 1024);

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        assertEquals("No downloads should have been queued for existing files", 0, downloads.size());
    }

    @Test
    public void testDownload_SizeMismatchWithManifest_DeletesFile() throws IOException
    {
        // Manifest says 1000 bytes, but S3 file is only 500 bytes
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(1000));
        BackupDescriptor desc = makeDescriptor(sstable);

        createS3File(desc, Component.PRIMARY_INDEX, 500);
        ensureDataFileInS3(desc);

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        assertFalse("File with wrong size should be deleted after download",
                    new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
    }

    @Test
    public void testDownload_TempFileCleanedUpBeforeDownload() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(64));
        BackupDescriptor desc = makeDescriptor(sstable);

        // Create a leftover .tmp file
        Path tmpFile = Path.of(desc.filenameFor(Component.PRIMARY_INDEX) + ".tmp");
        Files.createDirectories(tmpFile.getParent());
        Files.write(tmpFile, new byte[32]);
        assertTrue(".tmp file should exist before download", Files.exists(tmpFile));

        // Put correct file in S3
        createS3File(desc, Component.PRIMARY_INDEX, 64);
        ensureDataFileInS3(desc);

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        assertFalse("Leftover .tmp file should be cleaned up", Files.exists(tmpFile));
        assertTrue("Downloaded file should exist",
                   new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
    }

    // ---- Validate then re-download (retry) tests ----

    @Test
    public void testValidateThenDownload_TruncatedFileIsRedownloaded() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(100));
        BackupDescriptor desc = makeDescriptor(sstable);
        List<BackupDescriptor> descs = Collections.singletonList(desc);

        // Place a truncated file in cache
        createCachedFile(desc, Component.PRIMARY_INDEX, 50);

        // Place the correct file in S3
        createS3File(desc, Component.PRIMARY_INDEX, 100);
        ensureDataFileInS3(desc);

        // Step 1: validate deletes the truncated file
        ctx.validateCachedComponents(descs);
        assertFalse("Truncated file should be deleted by validation",
                    new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());

        // Step 2: download replaces it with the correct file
        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(descs);
        ctx.waitForDownloads(downloads);
        assertTrue("File should be re-downloaded after validation deleted it",
                   new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
        assertEquals("Re-downloaded file should have correct size",
                     100, new File(desc.filenameFor(Component.PRIMARY_INDEX)).length());
    }

    @Test
    public void testValidateThenDownload_AllComponentsRecoveredAfterTruncation() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(100));
        BackupDescriptor desc = makeDescriptor(sstable);
        List<BackupDescriptor> descs = Collections.singletonList(desc);

        // Place truncated files in cache for all components, correct files in S3
        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
        {
            createCachedFile(desc, comp, 50);
            createS3File(desc, comp, 100);
        }
        ensureDataFileInS3(desc);

        // Validate should delete all truncated files
        ctx.validateCachedComponents(descs);
        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            assertFalse("Truncated " + comp.name() + " should be deleted",
                        new File(desc.filenameFor(comp)).exists());

        // Download should re-fetch all
        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(descs);
        ctx.waitForDownloads(downloads);
        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
        {
            assertTrue(comp.name() + " should be re-downloaded",
                       new File(desc.filenameFor(comp)).exists());
            assertEquals(comp.name() + " should have correct size",
                         100, new File(desc.filenameFor(comp)).length());
        }
    }

    @Test
    public void testDownload_S3FailureDoesNotLeavePartialFile() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(100));
        BackupDescriptor desc = makeDescriptor(sstable);
        ensureDataFileInS3(desc);

        // Inject failures for all 6 component downloads
        for (int i = 0; i < BackupMemtableContext.COMPONENTS_TO_DOWNLOAD.length; i++)
            fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Failure<>(
                AwsAsyncS3FakeBackup.Method.GET_OBJECT_AS_FILE,
                new RuntimeException("S3 network error")));

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            assertFalse("Failed download should not leave " + comp.name(),
                        new File(desc.filenameFor(comp)).exists());
    }

    @Test
    public void testDownload_FailureThenRetry_Succeeds() throws IOException
    {
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(100));
        BackupDescriptor desc = makeDescriptor(sstable);
        List<BackupDescriptor> descs = Collections.singletonList(desc);

        // Put all correct files in S3
        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            createS3File(desc, comp, 100);
        ensureDataFileInS3(desc);

        // First attempt: inject failures for all component downloads
        for (int i = 0; i < BackupMemtableContext.COMPONENTS_TO_DOWNLOAD.length; i++)
            fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Failure<>(
                AwsAsyncS3FakeBackup.Method.GET_OBJECT_AS_FILE,
                new RuntimeException("Transient S3 error")));

        List<AsyncPromise<?>> downloads1 = ctx.downloadRequiredComponents(descs);
        ctx.waitForDownloads(downloads1);

        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            assertFalse("First attempt should fail for " + comp.name(),
                        new File(desc.filenameFor(comp)).exists());

        // Second attempt: no injection, should succeed from real fake S3
        List<AsyncPromise<?>> downloads2 = ctx.downloadRequiredComponents(descs);
        ctx.waitForDownloads(downloads2);

        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
        {
            assertTrue("Retry should download " + comp.name(),
                       new File(desc.filenameFor(comp)).exists());
            assertEquals(comp.name() + " should have correct size",
                         100, new File(desc.filenameFor(comp)).length());
        }
    }

    // ---- .len file resilience tests ----

    @Test
    public void testDownload_TruncatedLenFile_IsRewritten() throws IOException
    {
        // Reproduces the production scenario: a 0-byte .len left behind by a crash
        // mid-write. Pre-fix, exists() returned true and the corrupt file was kept,
        // breaking later read() in ForS3.<init>.
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(64));
        BackupDescriptor desc = makeDescriptor(sstable);

        // Pre-create cached components so only the .len needs rewriting
        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            createCachedFile(desc, comp, 64);
        ensureDataFileInS3(desc);

        // Put a 0-byte .len file in cache
        Path lenFile = Path.of(desc.filenameFor(Component.DATA) + ".len");
        Files.createDirectories(lenFile.getParent());
        Files.write(lenFile, new byte[0]);

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        File lenAsFile = new File(lenFile);
        assertTrue(".len file should exist after download", lenAsFile.exists());
        assertEquals(".len file should be the expected size",
                     BackupMemtableContext.DataLengthFileSerializer.FILE_SIZE,
                     lenAsFile.length());
        // The rewritten file should be readable as a valid 14-byte DL record
        assertEquals(1024L, BackupMemtableContext.DataLengthFileSerializer.read(lenFile.toString()));
    }

    @Test
    public void testDownload_WrongSizedLenFile_IsRewritten() throws IOException
    {
        // A .len file with a plausible but wrong length (not 14 bytes) should be
        // treated as corrupt and rewritten.
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(64));
        BackupDescriptor desc = makeDescriptor(sstable);

        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            createCachedFile(desc, comp, 64);
        ensureDataFileInS3(desc);

        Path lenFile = Path.of(desc.filenameFor(Component.DATA) + ".len");
        Files.createDirectories(lenFile.getParent());
        // 7 bytes — half-written
        Files.write(lenFile, new byte[] { 'D', 'L', 0, 0, 0, 0, 0 });

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        assertEquals(".len file should be the expected size after rewrite",
                     BackupMemtableContext.DataLengthFileSerializer.FILE_SIZE,
                     new File(lenFile).length());
        assertEquals(1024L, BackupMemtableContext.DataLengthFileSerializer.read(lenFile.toString()));
    }

    @Test
    public void testDownload_ValidLenFile_NotRewritten() throws IOException
    {
        // A pre-existing valid .len file should be left alone — no download queued.
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(64));
        BackupDescriptor desc = makeDescriptor(sstable);

        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            createCachedFile(desc, comp, 64);
        ensureDataFileInS3(desc);

        File lenFile = new File(desc.filenameFor(Component.DATA) + ".len");
        BackupMemtableContext.DataLengthFileSerializer.write(lenFile, 9999L);

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        assertEquals("No downloads should be queued when caches are complete and valid",
                     0, downloads.size());
        assertEquals("Valid .len content should be preserved",
                     9999L,
                     BackupMemtableContext.DataLengthFileSerializer.read(lenFile.path().toString()));
    }

    @Test
    public void testDownload_LeftoverLenTmpFile_CleanedUp() throws IOException
    {
        // A leftover .len.tmp from a previously interrupted write should be removed
        // when downloadRequiredComponents runs, mirroring the component .tmp cleanup.
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(64));
        BackupDescriptor desc = makeDescriptor(sstable);

        for (Component comp : BackupMemtableContext.COMPONENTS_TO_DOWNLOAD)
            createCachedFile(desc, comp, 64);
        ensureDataFileInS3(desc);

        Path lenTmp = Path.of(desc.filenameFor(Component.DATA) + ".len.tmp");
        Files.createDirectories(lenTmp.getParent());
        Files.write(lenTmp, new byte[] { 1, 2, 3 });
        assertTrue(".len.tmp should exist before download", Files.exists(lenTmp));

        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(Collections.singletonList(desc));
        ctx.waitForDownloads(downloads);

        assertFalse("Leftover .len.tmp should be cleaned up", Files.exists(lenTmp));
    }

    @Test
    public void testValidateThenDownload_ManifestSizeMismatchInS3_StillDetected() throws IOException
    {
        // Manifest says 1000, S3 has 500. Validate has nothing to check (no cached file).
        // Download fetches 500 bytes, post-download check catches mismatch with manifest.
        BackupManifest.BackupSSTable sstable = buildSSTable("nb-1-big", allComponentSizes(1000));
        BackupDescriptor desc = makeDescriptor(sstable);
        List<BackupDescriptor> descs = Collections.singletonList(desc);

        createS3File(desc, Component.PRIMARY_INDEX, 500);
        ensureDataFileInS3(desc);

        // Validate: nothing cached, nothing to do
        ctx.validateCachedComponents(descs);

        // Download: gets 500 bytes but manifest says 1000, should delete
        List<AsyncPromise<?>> downloads = ctx.downloadRequiredComponents(descs);
        ctx.waitForDownloads(downloads);

        assertFalse("File with manifest size mismatch should be deleted",
                    new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists());
    }
}
