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

package com.netflix.cassandra.virtual;

import java.io.File;
import java.io.IOException;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.cassandra.backups.AwsAsyncS3FakeBackup;
import com.netflix.cassandra.backups.BackupContext;
import com.netflix.cassandra.backups.BackupManifest;
import com.netflix.cassandra.db.virtual.BackupsTable;
import com.netflix.cassandra.db.virtual.NetflixViewsKeyspace;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BackupsTableTest extends CQLTester
{
    private static final String KS_NAME = NetflixViewsKeyspace.NAME;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private AwsAsyncS3FakeBackup fakeS3;
    private String fakeS3Root;
    private BackupsTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void config() throws IOException
    {
        fakeS3Root = tempFolder.newFolder("fake-s3").getAbsolutePath();

        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");
        fakeS3 = new AwsAsyncS3FakeBackup(Region.US_EAST_1);
        fakeS3.setFakeS3RootDir(fakeS3Root);

        table = new BackupsTable(KS_NAME, backupContext, fakeS3);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        // Create test keyspace and table
        schemaChange("CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        schemaChange("CREATE TABLE IF NOT EXISTS test_ks.test_table (id int PRIMARY KEY, value text)");

        disablePreparedReuseForTest();
    }

    @Test
    public void testSelectWithNoBackups() throws Throwable
    {
        String query = "SELECT * FROM " + KS_NAME + ".backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);
        assertFalse("Should return no rows when no backups exist", result.iterator().hasNext());
    }

    @Test
    public void testSelectWithSingleBackup() throws Throwable
    {
        // Create a backup manifest
        long timestamp = 1700000000000L;
        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");
        String metaPath = backupContext.metafilePrefix() + timestamp + "/test_ks/test_table/manifest.json";

        BackupManifest manifest = createTestManifest("test_app", "test_ks", "test_table", timestamp, 1000000L, true);
        writeManifestToFakeS3(backupContext.bucket(), metaPath, manifest);

        String query = "SELECT * FROM " + KS_NAME + ".backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);

        assertTrue("Should return at least one row", result.iterator().hasNext());
        Row row = result.one();
        assertEquals("test_ks", row.getString("keyspace_name"));
        assertEquals("test_table", row.getString("table_name"));
        assertEquals(timestamp, row.getLong("timestamp"));
        assertEquals("test_app", row.getString("app_name"));
        assertEquals(1000000L, row.getLong("total_size"));
        assertTrue(row.getBool("uploaded"));
    }

    @Test
    public void testSelectWithMultipleBackups() throws Throwable
    {
        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");

        // Create three backups at different timestamps
        long[] timestamps = {1700000000000L, 1700001000000L, 1700002000000L};
        long[] sizes = {1000000L, 2000000L, 3000000L};
        boolean[] uploaded = {true, false, true};

        for (int i = 0; i < timestamps.length; i++)
        {
            String metaPath = backupContext.metafilePrefix() + timestamps[i] + "/test_ks/test_table/manifest.json";
            BackupManifest manifest = createTestManifest("test_app", "test_ks", "test_table", timestamps[i], sizes[i], uploaded[i]);
            writeManifestToFakeS3(backupContext.bucket(), metaPath, manifest);
        }

        String query = "SELECT * FROM " + KS_NAME + ".backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);

        int count = 0;
        for (Row row : result)
        {
            assertTrue("Timestamp should be one of the expected values",
                      row.getLong("timestamp") >= timestamps[0] && row.getLong("timestamp") <= timestamps[2]);
            count++;
        }
        assertEquals("Should return three backups", 3, count);
    }

    @Test
    public void testExcludesMetadataFilesFromSizeAndUploadStatus() throws Throwable
    {
        long timestamp = 1700000000000L;
        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");
        String metaPath = backupContext.metafilePrefix() + timestamp + "/test_ks/test_table/manifest.json";

        // Create manifest with both actual SSTable components and metadata files
        BackupManifest manifest = BackupManifest.builder()
            .info(BackupManifest.Info.builder()
                .appName("test_app")
                .version(1)
                .build())
            .addData(BackupManifest.Data.builder()
                .keyspaceName("test_ks")
                .columnfamilyName("test_table")
                .addSstable(BackupManifest.BackupSSTable.builder()
                    .prefix("nb-1-big")
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("nb-1-big-Data.db")
                        .fileSizeOnDisk(500000L)
                        .isUploaded(true)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path/to/Data.db")
                        .build())
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("manifest.json")  // This should be excluded
                        .fileSizeOnDisk(1000L)
                        .isUploaded(false)  // Even though this is false, it shouldn't affect upload status
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path/to/manifest.json")
                        .build())
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("schema.cql")  // This should be excluded
                        .fileSizeOnDisk(500L)
                        .isUploaded(false)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path/to/schema.cql")
                        .build())
                    .build())
                .build())
            .build();

        writeManifestToFakeS3(backupContext.bucket(), metaPath, manifest);

        String query = "SELECT * FROM " + KS_NAME + ".backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);

        assertTrue("Should return one row", result.iterator().hasNext());
        Row row = result.one();

        // Size should only include Data.db (500000), not manifest.json or schema.cql
        assertEquals("Size should exclude metadata files", 500000L, row.getLong("total_size"));

        // Upload status should be true because only Data.db matters (and it's uploaded)
        assertTrue("Upload status should be true when only actual SSTable components are considered", row.getBool("uploaded"));
    }

    @Test
    public void testSelectWithInvalidKeyspace() throws Throwable
    {
        String query = "SELECT * FROM " + KS_NAME + ".backups WHERE keyspace_name = 'nonexistent_ks' AND table_name = 'test_table'";
        try
        {
            executeNet(query);
            fail("Should throw InvalidRequestException for nonexistent keyspace");
        }
        catch (Exception e)
        {
            assertTrue("Should be InvalidRequestException", e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testSelectFiltersCorrectKeyspaceAndTable() throws Throwable
    {
        // Create another keyspace and table
        schemaChange("CREATE KEYSPACE IF NOT EXISTS other_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        schemaChange("CREATE TABLE IF NOT EXISTS other_ks.other_table (id int PRIMARY KEY, value text)");

        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");
        long timestamp = 1700000000000L;

        // Create manifests for multiple keyspace/table combinations
        String metaPath1 = backupContext.metafilePrefix() + timestamp + "/test_ks/test_table/manifest.json";
        BackupManifest manifest1 = createTestManifest("test_app", "test_ks", "test_table", timestamp, 1000000L, true);
        writeManifestToFakeS3(backupContext.bucket(), metaPath1, manifest1);

        String metaPath2 = backupContext.metafilePrefix() + timestamp + "/other_ks/other_table/manifest.json";
        BackupManifest manifest2 = createTestManifest("test_app", "other_ks", "other_table", timestamp, 2000000L, true);
        writeManifestToFakeS3(backupContext.bucket(), metaPath2, manifest2);

        // Query for test_ks.test_table
        String query = "SELECT * FROM " + KS_NAME + ".backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);

        int count = 0;
        for (Row row : result)
        {
            assertEquals("test_ks", row.getString("keyspace_name"));
            assertEquals("test_table", row.getString("table_name"));
            assertEquals(1000000L, row.getLong("total_size"));
            count++;
        }
        assertEquals("Should return only one backup for test_ks.test_table", 1, count);
    }

    private BackupManifest createTestManifest(String appName, String keyspace, String table, long timestamp, long fileSize, boolean uploaded)
    {
        return BackupManifest.builder()
            .info(BackupManifest.Info.builder()
                .appName(appName)
                .version(1)
                .build())
            .addData(BackupManifest.Data.builder()
                .keyspaceName(keyspace)
                .columnfamilyName(table)
                .addSstable(BackupManifest.BackupSSTable.builder()
                    .prefix("nb-1-big")
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("nb-1-big-Data.db")
                        .fileSizeOnDisk(fileSize)
                        .isUploaded(uploaded)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path/to/file")
                        .build())
                    .build())
                .build())
            .build();
    }

    private void writeManifestToFakeS3(String bucket, String key, BackupManifest manifest) throws IOException
    {
        File file = new File(fakeS3Root, bucket + "/" + key);
        file.getParentFile().mkdirs();
        mapper.writeValue(file, manifest);
    }
}