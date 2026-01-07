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

package org.apache.cassandra.distributed.test.netflix;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.cassandra.importing.ImportJobManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.util.File;

public class ImportJobCleanupTest extends RemoteImportTestBase
{
    @Test
    public void testCleanupOrphanedJobsAndDirectories() throws Exception
    {
        try (Cluster cluster = setupTestCluster(3, 1))
        {
            // Create the test_table if it doesn't exist
            cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + TEST_KEYSPACE + ".test_table (id int PRIMARY KEY, data text)");

            // Insert some test data to create SSTables
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + TEST_KEYSPACE + ".test_table (id, data) VALUES (?, ?)",
                                             org.apache.cassandra.distributed.api.ConsistencyLevel.ONE,
                                             i, "test_data_" + i);
            }

            testOrphanedJobCleanup(cluster);
            testOrphanedDirectoryCleanup(cluster);
        }
    }

    private void testOrphanedJobCleanup(Cluster cluster) throws Exception
    {
        logger.info("Testing orphaned job cleanup");

        // Create a remote import entry
        UUID importId = UUID.randomUUID();
        String insertQuery = "INSERT INTO system_distributed.remote_import " +
                           "(id, target_keyspace, target_table, state, source, size) " +
                           "VALUES (?, ?, ?, ?, ?, ?)";

        cluster.coordinator(1).execute(insertQuery, org.apache.cassandra.distributed.api.ConsistencyLevel.ONE,
                                     importId, TEST_KEYSPACE, "test_table", "pending",
                                     "http://example.com/test.zip", 1024L);

        // Verify the entry was created
        Object[][] results = cluster.coordinator(1).execute(
            "SELECT id FROM system_distributed.remote_import WHERE id = ? AND target_keyspace = ? AND target_table = ?",
            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE,
            importId, TEST_KEYSPACE, "test_table");
        Assert.assertEquals("Import entry should exist", 1, results.length);

        // Create a job in the ImportJobManager (simulate job creation)
        cluster.get(1).callOnInstance(() -> {
            ImportJobManager manager = ImportJobManager.getInstance();
            // Trigger job creation by calling getOrCreateJob
            manager.getOrCreateJob(importId, TEST_KEYSPACE, "test_table");

            // Verify job exists in the manager
            Assert.assertNotNull("Job should exist in manager", manager.getJob(importId));
            logger.info("Created job {} in ImportJobManager", importId);
            return null;
        });

        // Delete the remote import entry (simulating cleanup)
        cluster.coordinator(1).execute(
            "DELETE FROM system_distributed.remote_import WHERE id = ? AND target_keyspace = ? AND target_table = ?",
            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE,
            importId, TEST_KEYSPACE, "test_table");

        // Verify entry was deleted
        results = cluster.coordinator(1).execute(
            "SELECT id FROM system_distributed.remote_import WHERE id = ? AND target_keyspace = ? AND target_table = ?",
            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE,
            importId, TEST_KEYSPACE, "test_table");
        Assert.assertEquals("Import entry should be deleted", 0, results.length);

        // Run the cleanup task
        cluster.get(1).callOnInstance(() -> {
            ImportJobManager manager = ImportJobManager.getInstance();

            // Verify job still exists before cleanup
            Assert.assertNotNull("Job should still exist before cleanup", manager.getJob(importId));

            // Run the cleanup
            manager.cleanupOrphanedJobs();

            // Verify job was removed after cleanup
            Assert.assertNull("Job should be removed after cleanup", manager.getJob(importId));
            logger.info("Successfully cleaned up orphaned job {}", importId);
            return null;
        });
    }

    private void testOrphanedDirectoryCleanup(Cluster cluster) throws Exception
    {
        logger.info("Testing orphaned directory cleanup");

        // Create fake UUIDs that we know won't be in the database
        UUID fakeJobId1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID fakeJobId2 = UUID.fromString("00000000-0000-0000-0000-000000000002");

        cluster.get(1).callOnInstance(() -> {
            try
            {
                // Get the data directory for our test table
                ColumnFamilyStore cfs = Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore("test_table");
                Directories dirs = cfs.getDirectories();
                File dataDir = dirs.getCFDirectories().get(0);

                // Create imports directory structure
                File importsDir = new File(dataDir, "imports");
                importsDir.tryCreateDirectories();

                // Create fake job directories with some files
                File fakeJobDir1 = new File(importsDir, fakeJobId1.toString());
                File fakeJobDir2 = new File(importsDir, fakeJobId2.toString());

                fakeJobDir1.tryCreateDirectories();
                fakeJobDir2.tryCreateDirectories();

                // Create some fake files in these directories
                File fakeFile1 = new File(fakeJobDir1, "fake_data.db");
                File fakeFile2 = new File(fakeJobDir2, "fake_index.db");
                File subDir = new File(fakeJobDir2, "subdir");
                subDir.tryCreateDirectories();
                File nestedFile = new File(subDir, "nested_file.txt");

                // Write some content to the files
                try (java.io.FileOutputStream fos1 = new java.io.FileOutputStream(fakeFile1.toJavaIOFile()))
                {
                    fos1.write("fake sstable data".getBytes());
                }
                try (java.io.FileOutputStream fos2 = new java.io.FileOutputStream(fakeFile2.toJavaIOFile()))
                {
                    fos2.write("fake index data".getBytes());
                }
                try (java.io.FileOutputStream fos3 = new java.io.FileOutputStream(nestedFile.toJavaIOFile()))
                {
                    fos3.write("nested fake data".getBytes());
                }

                // Also create a non-UUID directory that should be ignored
                File nonUuidDir = new File(importsDir, "not_a_uuid");
                nonUuidDir.tryCreateDirectories();
                File nonUuidFile = new File(nonUuidDir, "should_remain.txt");
                try (java.io.FileOutputStream fos = new java.io.FileOutputStream(nonUuidFile.toJavaIOFile()))
                {
                    fos.write("this should not be deleted".getBytes());
                }

                logger.info("Created fake import directories: {}, {}, {}",
                           fakeJobDir1.absolutePath(), fakeJobDir2.absolutePath(), nonUuidDir.absolutePath());

                // Verify directories exist before cleanup
                Assert.assertTrue("Fake job dir 1 should exist", fakeJobDir1.exists());
                Assert.assertTrue("Fake job dir 2 should exist", fakeJobDir2.exists());
                Assert.assertTrue("Non-UUID dir should exist", nonUuidDir.exists());
                Assert.assertTrue("Fake file 1 should exist", fakeFile1.exists());
                Assert.assertTrue("Fake file 2 should exist", fakeFile2.exists());
                Assert.assertTrue("Nested file should exist", nestedFile.exists());
                Assert.assertTrue("Non-UUID file should exist", nonUuidFile.exists());

                // Wait for directories to become older than the minimum age (1s + buffer)
                logger.info("Waiting for directories to age past minimum cleanup age...");

                return null;
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to setup orphaned directories", e);
            }
        });

        // Sleep to ensure directories are old enough to be cleaned up
        Thread.sleep(2000); // 2 seconds to be safe (min age is 1s)

        cluster.get(1).callOnInstance(() -> {
            try
            {
                // Recreate file references since we're in a new callOnInstance block
                ColumnFamilyStore cfs = Keyspace.open(TEST_KEYSPACE).getColumnFamilyStore("test_table");
                Directories dirs = cfs.getDirectories();
                File dataDir = dirs.getCFDirectories().get(0);
                File importsDir = new File(dataDir, "imports");

                File fakeJobDir1 = new File(importsDir, fakeJobId1.toString());
                File fakeJobDir2 = new File(importsDir, fakeJobId2.toString());
                File nonUuidDir = new File(importsDir, "not_a_uuid");
                File nonUuidFile = new File(nonUuidDir, "should_remain.txt");

                // Run the cleanup
                ImportJobManager manager = ImportJobManager.getInstance();
                logger.info("Running cleanup with fake job directories: {} and {}", fakeJobId1, fakeJobId2);
                manager.cleanupOrphanedJobs();

                // Check if directories still exist for debugging
                logger.info("After cleanup - fakeJobDir1 exists: {}, fakeJobDir2 exists: {}",
                           fakeJobDir1.exists(), fakeJobDir2.exists());

                // Verify fake UUID directories were removed
                Assert.assertFalse("Fake job dir 1 should be removed", fakeJobDir1.exists());
                Assert.assertFalse("Fake job dir 2 should be removed", fakeJobDir2.exists());

                // Verify non-UUID directory was left alone
                Assert.assertTrue("Non-UUID dir should remain", nonUuidDir.exists());
                Assert.assertTrue("Non-UUID file should remain", nonUuidFile.exists());

                logger.info("Successfully cleaned up fake import directories while preserving non-UUID directories");

                // Clean up the non-UUID directory for test cleanliness
                nonUuidFile.delete();
                nonUuidDir.delete();
                return null;
            }
            catch (Exception e)
            {
                logger.error("Error during directory cleanup test", e);
                throw new RuntimeException(e);
            }
        });
    }
}
