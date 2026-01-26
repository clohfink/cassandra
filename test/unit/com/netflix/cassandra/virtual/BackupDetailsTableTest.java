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
import com.netflix.cassandra.db.virtual.BackupDetailsTable;
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

public class BackupDetailsTableTest extends CQLTester
{
    private static final String KS_NAME = NetflixViewsKeyspace.NAME;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private AwsAsyncS3FakeBackup fakeS3;
    private String fakeS3Root;
    private BackupDetailsTable table;

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

        table = new BackupDetailsTable(KS_NAME, backupContext, fakeS3);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        // Create test keyspace and table
        schemaChange("CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        schemaChange("CREATE TABLE IF NOT EXISTS test_ks.test_table (id int PRIMARY KEY, value text)");

        disablePreparedReuseForTest();
    }

    @Test
    public void testSelectWithNoBackups() throws Throwable
    {
        String query = "SELECT * FROM " + KS_NAME + ".backup_details " +
                       "WHERE keyspace_name = 'test_ks' AND table_name = 'test_table' AND timestamp = 1700000000000";
        ResultSet result = executeNet(query);
        assertFalse("Should return no rows when no backups exist", result.iterator().hasNext());
    }

    @Test
    public void testSelectWithSingleSSTable() throws Throwable
    {
        long timestamp = 1700000000000L;
        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");
        String metaPath = backupContext.metafilePrefix() + timestamp + "/test_ks/test_table/manifest.json";

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
                        .fileSizeOnDisk(100000L)
                        .isUploaded(true)
                        .compression("LZ4")
                        .encryption("AES256")
                        .backupPath("s3://bucket/path/to/Data.db")
                        .build())
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("nb-1-big-Index.db")
                        .fileSizeOnDisk(50000L)
                        .isUploaded(true)
                        .compression("LZ4")
                        .encryption("AES256")
                        .backupPath("s3://bucket/path/to/Index.db")
                        .build())
                    .build())
                .build())
            .build();

        writeManifestToFakeS3(backupContext.bucket(), metaPath, manifest);

        String query = "SELECT * FROM " + KS_NAME + ".backup_details " +
                       "WHERE keyspace_name = 'test_ks' AND table_name = 'test_table' AND timestamp = " + timestamp;
        ResultSet result = executeNet(query);

        int count = 0;
        for (Row row : result)
        {
            assertEquals("test_ks", row.getString("keyspace_name"));
            assertEquals("test_table", row.getString("table_name"));
            assertEquals(timestamp, row.getLong("timestamp"));
            assertEquals("nb-1-big", row.getString("sstable_prefix"));

            String componentName = row.getString("component_name");
            assertTrue("Component name should be Data.db or Index.db",
                      componentName.equals("nb-1-big-Data.db") || componentName.equals("nb-1-big-Index.db"));

            if (componentName.equals("nb-1-big-Data.db"))
            {
                assertEquals(100000L, row.getLong("file_size"));
            }
            else
            {
                assertEquals(50000L, row.getLong("file_size"));
            }

            assertTrue(row.getBool("uploaded"));
            assertEquals("LZ4", row.getString("compression"));
            assertEquals("AES256", row.getString("encryption"));
            assertTrue(row.getString("backup_path").startsWith("s3://bucket/path/to/"));

            count++;
        }
        assertEquals("Should return two rows (one for each component)", 2, count);
    }

    @Test
    public void testSelectWithMultipleSSTablesAndComponents() throws Throwable
    {
        long timestamp = 1700000000000L;
        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");
        String metaPath = backupContext.metafilePrefix() + timestamp + "/test_ks/test_table/manifest.json";

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
                        .fileSizeOnDisk(100000L)
                        .isUploaded(true)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path1")
                        .build())
                    .build())
                .addSstable(BackupManifest.BackupSSTable.builder()
                    .prefix("nb-2-big")
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("nb-2-big-Data.db")
                        .fileSizeOnDisk(200000L)
                        .isUploaded(false)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path2")
                        .build())
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("nb-2-big-Index.db")
                        .fileSizeOnDisk(150000L)
                        .isUploaded(true)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path3")
                        .build())
                    .build())
                .build())
            .build();

        writeManifestToFakeS3(backupContext.bucket(), metaPath, manifest);

        String query = "SELECT * FROM " + KS_NAME + ".backup_details " +
                       "WHERE keyspace_name = 'test_ks' AND table_name = 'test_table' AND timestamp = " + timestamp;
        ResultSet result = executeNet(query);

        int count = 0;
        boolean foundNb1Data = false;
        boolean foundNb2Data = false;
        boolean foundNb2Index = false;

        for (Row row : result)
        {
            String sstablePrefix = row.getString("sstable_prefix");
            String componentName = row.getString("component_name");

            if (sstablePrefix.equals("nb-1-big") && componentName.equals("nb-1-big-Data.db"))
            {
                foundNb1Data = true;
                assertEquals(100000L, row.getLong("file_size"));
                assertTrue(row.getBool("uploaded"));
            }
            else if (sstablePrefix.equals("nb-2-big") && componentName.equals("nb-2-big-Data.db"))
            {
                foundNb2Data = true;
                assertEquals(200000L, row.getLong("file_size"));
                assertFalse(row.getBool("uploaded"));
            }
            else if (sstablePrefix.equals("nb-2-big") && componentName.equals("nb-2-big-Index.db"))
            {
                foundNb2Index = true;
                assertEquals(150000L, row.getLong("file_size"));
                assertTrue(row.getBool("uploaded"));
            }

            count++;
        }

        assertEquals("Should return three components", 3, count);
        assertTrue("Should find nb-1-big Data.db", foundNb1Data);
        assertTrue("Should find nb-2-big Data.db", foundNb2Data);
        assertTrue("Should find nb-2-big Index.db", foundNb2Index);
    }

    @Test
    public void testExcludesMetadataFiles() throws Throwable
    {
        long timestamp = 1700000000000L;
        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");
        String metaPath = backupContext.metafilePrefix() + timestamp + "/test_ks/test_table/manifest.json";

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
                        .fileSizeOnDisk(100000L)
                        .isUploaded(true)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path/data")
                        .build())
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("manifest.json")  // Should be excluded
                        .fileSizeOnDisk(1000L)
                        .isUploaded(true)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path/manifest")
                        .build())
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName("schema.cql")  // Should be excluded
                        .fileSizeOnDisk(500L)
                        .isUploaded(true)
                        .compression("NONE")
                        .encryption("PLAINTEXT")
                        .backupPath("path/schema")
                        .build())
                    .build())
                .build())
            .build();

        writeManifestToFakeS3(backupContext.bucket(), metaPath, manifest);

        String query = "SELECT * FROM " + KS_NAME + ".backup_details " +
                       "WHERE keyspace_name = 'test_ks' AND table_name = 'test_table' AND timestamp = " + timestamp;
        ResultSet result = executeNet(query);

        int count = 0;
        for (Row row : result)
        {
            String componentName = row.getString("component_name");
            assertFalse("Should not include manifest.json", componentName.equals("manifest.json"));
            assertFalse("Should not include schema.cql", componentName.equals("schema.cql"));
            assertEquals("Should only include Data.db", "nb-1-big-Data.db", componentName);
            count++;
        }

        assertEquals("Should return only one component (Data.db)", 1, count);
    }

    @Test
    public void testSelectWithInvalidKeyspace() throws Throwable
    {
        String query = "SELECT * FROM " + KS_NAME + ".backup_details " +
                       "WHERE keyspace_name = 'nonexistent_ks' AND table_name = 'test_table' AND timestamp = 1700000000000";
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
    public void testSelectFiltersCorrectKeyspaceTableAndTimestamp() throws Throwable
    {

        BackupContext backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");

        long timestamp1 = 1700000000000L;
        long timestamp2 = 1700001000000L;

        // Create manifest for timestamp1
        String metaPath1 = backupContext.metafilePrefix() + timestamp1 + "/test_ks/test_table/manifest.json";
        BackupManifest manifest1 = createSimpleManifest("test_ks", "test_table", "nb-1-big");
        writeManifestToFakeS3(backupContext.bucket(), metaPath1, manifest1);

        // Create manifest for timestamp2
        String metaPath2 = backupContext.metafilePrefix() + timestamp2 + "/test_ks/test_table/manifest.json";
        BackupManifest manifest2 = createSimpleManifest("test_ks", "test_table", "nb-2-big");
        writeManifestToFakeS3(backupContext.bucket(), metaPath2, manifest2);

        // Query for timestamp1 only
        String query = "SELECT * FROM " + KS_NAME + ".backup_details " +
                       "WHERE keyspace_name = 'test_ks' AND table_name = 'test_table' AND timestamp = " + timestamp1;
        ResultSet result = executeNet(query);

        int count = 0;
        for (Row row : result)
        {
            assertEquals(timestamp1, row.getLong("timestamp"));
            assertEquals("nb-1-big", row.getString("sstable_prefix"));
            count++;
        }
        assertEquals("Should return only components from timestamp1", 1, count);
    }

    private BackupManifest createSimpleManifest(String keyspace, String table, String sstablePrefix)
    {
        return BackupManifest.builder()
            .info(BackupManifest.Info.builder()
                .appName("test_app")
                .version(1)
                .build())
            .addData(BackupManifest.Data.builder()
                .keyspaceName(keyspace)
                .columnfamilyName(table)
                .addSstable(BackupManifest.BackupSSTable.builder()
                    .prefix(sstablePrefix)
                    .addSstableComponent(BackupManifest.BackupSSTableComponent.builder()
                        .fileName(sstablePrefix + "-Data.db")
                        .fileSizeOnDisk(100000L)
                        .isUploaded(true)
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