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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Test;

import com.netflix.cassandra.backups.AwsAsyncS3FakeBackup;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BackupMemtableErrorTest extends BackupMemtableTestBase
{
    @Test
    public void testBackupMemtable_ErrorConditions() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .start()))
        {
            setupTable(cluster, "DESC");
            insertTestData(cluster);
            setupBackupMemtable(cluster);

            // Test key does not exist
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT sub_id, value FROM test.test_table WHERE id = -1", ConsistencyLevel.ALL);
            assertEquals("Expected no rows, but got some", 0, result.toObjectArrays().length);

            // Test insert fails
            try
            {
                cluster.coordinator(1).executeWithResult("INSERT INTO test.test_table (id, sub_id) VALUES (1, 2)", ConsistencyLevel.ALL);
                fail("Expected an exception when trying to insert into backup memtable");
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage().contains("Backup memtable cannot be used for updates or deletions"));
            }

            // Test range query fails
            try
            {
                cluster.coordinator(1).executeWithResult("SELECT id FROM test.test_table", ConsistencyLevel.ALL);
                fail("Expected an exception when trying to select range into backup memtable");
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage().contains("Range queries are not supported on S3 tables"));
            }
        }
    }

    @Test
    public void testBackupMemtable_invalidS3StateDoesNotHangCluster() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .start()))
        {
            setupTable(cluster, "DESC");
            insertTestData(cluster);

            // Simulate bad range read.
            setupBackupMemtable(
                cluster,
                new AwsAsyncS3FakeBackup.Injection.Value<>(AwsAsyncS3FakeBackup.Method.GET_OBJECT_RANGE_INTO_BUFFER, new byte[0])
            );

            // Read fails due to bad data.
            try
            {
                cluster.coordinator(1).executeWithResult("SELECT sub_id, value FROM test.test_table WHERE id = 1", ConsistencyLevel.ALL);
                fail("Expected an exception when trying to read from backup memtable");
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage().contains("Operation failed - received 0 responses and 1 failures"));
            }

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT sub_id, value FROM test.test_table WHERE id = 1", ConsistencyLevel.ALL);
            assertTrue(result.toObjectArrays().length > 0);
        }
    }

    @Test
    public void testBackupMemtableQuery_largeRows() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE test.test_table (id uuid, sub_id uuid, content text, PRIMARY KEY ((id), sub_id)) WITH CLUSTERING ORDER BY (sub_id DESC)");

            String insertStatement = "INSERT INTO test.test_table (id, sub_id, content) VALUES (?, ?, ?)";
            UUID largeRecordId = UUID.randomUUID();
            UUID largeRecordSubId = UUID.randomUUID();
            String largeRecordValue = generateRandomStringOfByteLength(1024 * 1024);
            // Assume 10x compression and create large payloads (100kb).
            cluster.coordinator(1).execute(insertStatement, ConsistencyLevel.ALL, largeRecordId, largeRecordSubId, largeRecordValue);
            // Also create a row with larger payloads. 1 MB.
            UUID largeRowPartitionId = UUID.randomUUID();
            List<String> expectedValues = new ArrayList<>();
            for(int i = 0; i < 10; i++) {
                String value = generateRandomStringOfByteLength(1024 * 1024 + i * 1024);
                expectedValues.add(value);
                cluster.coordinator(1).execute(insertStatement, ConsistencyLevel.ALL, largeRowPartitionId, UUID.randomUUID(), value);
            }

            cluster.get(1).nodetool("flush", "test");

            // Query backup memtable.
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM test.test_table WHERE id = ?", ConsistencyLevel.ALL, largeRecordId);
            assertQueryResults(result.toObjectArrays(),
                               new Object[][]{ { largeRecordId, largeRecordSubId, largeRecordValue } });

            result = cluster.coordinator(1).executeWithResult("SELECT * FROM test.test_table WHERE id = ?", ConsistencyLevel.ALL, largeRowPartitionId);
            List<Object> allValues = Arrays.stream(result.toObjectArrays()).map(row -> row[2]).collect(Collectors.toList());
            assertEquals("Expected 10 rows for large row partition", 10, allValues.size());
            assertTrue("Expected values are not fully present in the result", allValues.containsAll(expectedValues));
        }
    }
}