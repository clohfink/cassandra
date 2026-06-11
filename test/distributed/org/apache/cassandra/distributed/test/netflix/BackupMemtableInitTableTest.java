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

import java.io.IOException;

import org.junit.Test;

import com.netflix.cassandra.backups.AwsAsyncS3FakeBackup;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Distributed tests for the {@code netflix_views.backup_memtable_init} virtual table.
 * Verifies that the table reports the lifecycle of a {@code BackupMemtable}:
 *  - READY after successful init
 *  - FAILED when S3 access fails, with last_error populated and attempt_count {@literal >} 1
 *  - empty when no table uses a BackupMemtable
 */
public class BackupMemtableInitTableTest extends BackupMemtableTestBase
{
    private static final String SELECT_ALL =
        "SELECT keyspace_name, table_name, state, timestamp, bucket, prefix, " +
        "expected_descriptors, ready_descriptors, pending_downloads, attempt_count, " +
        "last_error " +
        "FROM netflix_views.backup_memtable_init " +
        "WHERE keyspace_name = 'test' AND table_name = 'test_table'";

    @Test
    public void testBackupMemtableInit_readyAfterSuccessfulInit() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .start()))
        {
            setupTable(cluster, "DESC");
            insertTestData(cluster);

            // Before the ALTER TABLE the test_table uses the default memtable, so the
            // virtual table should have no row for it.
            SimpleQueryResult empty = cluster.get(1).executeInternalWithResult(SELECT_ALL);
            assertEquals("Expected no rows before BackupMemtable is configured",
                         0, empty.toObjectArrays().length);

            setupBackupMemtable(cluster);

            // Trigger a read so init completes (reads block on initializationFuture)
            cluster.coordinator(1).executeWithResult(
                "SELECT sub_id, value FROM test.test_table WHERE id = 1", ConsistencyLevel.ALL);

            SimpleQueryResult result = cluster.get(1).executeInternalWithResult(SELECT_ALL);
            Object[][] rows = result.toObjectArrays();
            assertEquals("Expected exactly one row for test.test_table", 1, rows.length);

            // Column order matches SELECT_ALL above
            assertEquals("test", rows[0][0]);
            assertEquals("test_table", rows[0][1]);
            assertEquals("READY", rows[0][2]);
            assertNotNull("timestamp should be populated", rows[0][3]);
            assertEquals("testbucket", rows[0][4]);
            assertNotNull("prefix should be populated", rows[0][5]);
            int expectedDescriptors = (int) rows[0][6];
            int readyDescriptors = (int) rows[0][7];
            assertTrue("expected_descriptors should be > 0, was " + expectedDescriptors,
                       expectedDescriptors > 0);
            assertEquals("ready_descriptors should equal expected_descriptors on READY",
                         expectedDescriptors, readyDescriptors);
            assertEquals("pending_downloads should be 0 on READY", 0, rows[0][8]);
            assertTrue("attempt_count should be >= 1", (int) rows[0][9] >= 1);
            assertNull("last_error should be null on READY", rows[0][10]);
        }
    }

    @Test
    public void testBackupMemtableInit_failedReportsErrorAndRetries() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .start()))
        {
            setupTable(cluster, "DESC");
            insertTestData(cluster);

            // Inject enough failures to defeat the 3-retry meta download for both
            // BackupMemtable instances created by ALTER TABLE and TRUNCATE.
            AwsAsyncS3FakeBackup.Injection<?>[] failures = new AwsAsyncS3FakeBackup.Injection[12];
            for (int i = 0; i < failures.length; i++)
            {
                failures[i] = new AwsAsyncS3FakeBackup.Injection.Failure<>(
                    AwsAsyncS3FakeBackup.Method.GET_OBJECT_AS_FILE,
                    new RuntimeException("Simulated S3 failure"));
            }
            setupBackupMemtable(cluster, failures);

            // Read attempts initialization; it should fail synchronously.
            try
            {
                cluster.coordinator(1).executeWithResult(
                    "SELECT sub_id, value FROM test.test_table WHERE id = 1", ConsistencyLevel.ALL);
                fail("Expected read failure when initialization fails");
            }
            catch (Exception e)
            {
                // Expected — init failed.
            }

            // Poll the vtable until a FAILED row shows up (init runs on Stage.NETFLIX).
            Object[] row = pollForRow(cluster, 5_000);
            assertNotNull("Expected a row in backup_memtable_init", row);
            assertEquals("FAILED", row[2]);
            assertNotNull("last_error should be populated on FAILED", row[10]);
            assertTrue("last_error should mention failure, was: " + row[10],
                       row[10].toString().toLowerCase().contains("fail")
                       || row[10].toString().toLowerCase().contains("exception"));
            assertTrue("attempt_count should be >= 1 on FAILED", (int) row[9] >= 1);
        }
    }

    private static Object[] pollForRow(Cluster cluster, long timeoutMs)
    {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Object[] last = null;
        while (System.currentTimeMillis() < deadline)
        {
            SimpleQueryResult result = cluster.get(1).executeInternalWithResult(SELECT_ALL);
            Object[][] rows = result.toObjectArrays();
            if (rows.length > 0)
            {
                last = rows[0];
                if ("FAILED".equals(last[2]) || "READY".equals(last[2]))
                    return last;
            }
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return last;
            }
        }
        return last;
    }
}
