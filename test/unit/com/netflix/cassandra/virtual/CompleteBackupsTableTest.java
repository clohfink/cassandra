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
import com.netflix.cassandra.db.virtual.CompleteBackupsTable;
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

public class CompleteBackupsTableTest extends CQLTester
{
    private static final String KS_NAME = NetflixViewsKeyspace.NAME;
    private static final ObjectMapper mapper = new ObjectMapper();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private AwsAsyncS3FakeBackup fakeS3;
    private String fakeS3Root;
    private BackupContext backupContext;

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

        backupContext = new BackupContext("test", "us-east-1", "test_app", "-123456789");
        fakeS3 = new AwsAsyncS3FakeBackup(Region.US_EAST_1);
        fakeS3.setFakeS3RootDir(fakeS3Root);

        // Register both backups (per-node) and complete_backups (distributed aggregator)
        BackupsTable backupsTable = new BackupsTable(KS_NAME, backupContext, fakeS3);
        CompleteBackupsTable completeBackupsTable = new CompleteBackupsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(backupsTable, completeBackupsTable)));

        schemaChange("CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        schemaChange("CREATE TABLE IF NOT EXISTS test_ks.test_table (id int PRIMARY KEY, value text)");

        disablePreparedReuseForTest();
    }

    @Test
    public void testSelectWithNoBackups() throws Throwable
    {
        String query = "SELECT * FROM " + KS_NAME + ".complete_backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);
        assertFalse("Should return no rows when no backups exist", result.iterator().hasNext());
    }

    @Test
    public void testSingleBackupTimestamp() throws Throwable
    {
        long timestamp = 1700000000000L;
        writeManifest(timestamp, "test_ks", "test_table", 1000000L, true);

        String query = "SELECT * FROM " + KS_NAME + ".complete_backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);

        assertTrue("Should return at least one row", result.iterator().hasNext());
        Row row = result.one();
        assertEquals("test_ks", row.getString("keyspace_name"));
        assertEquals("test_table", row.getString("table_name"));
        assertEquals(timestamp, row.getLong("timestamp"));
        assertEquals(1000000L, row.getLong("total_size"));
        // Single node test - only one token
        assertEquals(1, row.getInt("num_tokens"));
        assertEquals(1, row.getInt("num_uploaded"));
        assertTrue(row.getBool("uploaded"));
    }

    @Test
    public void testNotUploadedBackup() throws Throwable
    {
        long timestamp = 1700000000000L;
        writeManifest(timestamp, "test_ks", "test_table", 500000L, false);

        String query = "SELECT * FROM " + KS_NAME + ".complete_backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);

        assertTrue("Should return one row", result.iterator().hasNext());
        Row row = result.one();
        assertEquals(timestamp, row.getLong("timestamp"));
        assertEquals(1, row.getInt("num_tokens"));
        assertEquals(0, row.getInt("num_uploaded"));
        assertFalse("Should not be uploaded", row.getBool("uploaded"));
    }

    @Test
    public void testMultipleTimestamps() throws Throwable
    {
        long ts1 = 1700000000000L;
        long ts2 = 1700001000000L;
        long ts3 = 1700002000000L;

        writeManifest(ts1, "test_ks", "test_table", 1000000L, true);
        writeManifest(ts2, "test_ks", "test_table", 2000000L, false);
        writeManifest(ts3, "test_ks", "test_table", 3000000L, true);

        String query = "SELECT * FROM " + KS_NAME + ".complete_backups WHERE keyspace_name = 'test_ks' AND table_name = 'test_table'";
        ResultSet result = executeNet(query);

        int count = 0;
        for (Row row : result)
        {
            long ts = row.getLong("timestamp");
            if (ts == ts1)
            {
                assertEquals(1000000L, row.getLong("total_size"));
                assertTrue(row.getBool("uploaded"));
            }
            else if (ts == ts2)
            {
                assertEquals(2000000L, row.getLong("total_size"));
                assertFalse(row.getBool("uploaded"));
            }
            else if (ts == ts3)
            {
                assertEquals(3000000L, row.getLong("total_size"));
                assertTrue(row.getBool("uploaded"));
            }
            count++;
        }
        assertEquals("Should return three rows", 3, count);
    }

    @Test
    public void testSelectWithInvalidKeyspace() throws Throwable
    {
        String query = "SELECT * FROM " + KS_NAME + ".complete_backups WHERE keyspace_name = 'nonexistent_ks' AND table_name = 'test_table'";
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

    private void writeManifest(long timestamp, String keyspace, String tableName,
                               long fileSize, boolean uploaded) throws IOException
    {
        String bucket = backupContext.bucket();
        String metaPath = backupContext.metafilePrefix() + timestamp + "/" + keyspace + "/" + tableName + "/manifest.json";

        BackupManifest manifest = BackupManifest.builder()
            .info(BackupManifest.Info.builder()
                .appName("test_app")
                .version(1)
                .build())
            .addData(BackupManifest.Data.builder()
                .keyspaceName(keyspace)
                .columnfamilyName(tableName)
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

        File file = new File(fakeS3Root, bucket + "/" + metaPath);
        file.getParentFile().mkdirs();
        mapper.writeValue(file, manifest);
    }
}
