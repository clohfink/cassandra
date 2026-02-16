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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;

import static org.junit.Assert.assertEquals;

public class BackupMemtableInterleavedTest extends BackupMemtableTestBase
{
    @Test
    public void testBackupMemtable_interleavedSSTables() throws IOException
    {
        for(String clusteringOrder : CLUSTERING_ORDERS)
        {
            try (Cluster cluster = init(Cluster.build(1)
                                               .withDataDirCount(1)
                                               .start()))
            {
                // Create table with small compression chunk size and additional column for random data
                cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
                cluster.schemaChange("CREATE TABLE IF NOT EXISTS test.test_table (id int, sub_id int, value text, random_data blob, PRIMARY KEY (id, sub_id)) " +
                                   "WITH CLUSTERING ORDER BY (sub_id " + clusteringOrder + ") " +
                                   "AND compression = {'chunk_length_in_kb': '1', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");

                Random random = new Random(42); // Use fixed seed for reproducibility

                // Write odd clustering keys (1, 3, 5, ..., 199) to first sstable
                for (int i = 1; i < 200; i += 2)
                {
                    byte[] randomBytes = new byte[100];
                    random.nextBytes(randomBytes);
                    cluster.coordinator(1).execute("INSERT INTO test.test_table (id, sub_id, value, random_data) VALUES (?, ?, ?, ?)",
                                                   ConsistencyLevel.ALL, 1, i, "test 1-" + i, ByteBuffer.wrap(randomBytes));
                }

                // Flush to create first sstable
                cluster.get(1).nodetool("flush", "test");

                // Write even clustering keys (0, 2, 4, ..., 198) to second sstable
                for (int i = 0; i < 200; i += 2)
                {
                    byte[] randomBytes = new byte[100];
                    random.nextBytes(randomBytes);
                    cluster.coordinator(1).execute("INSERT INTO test.test_table (id, sub_id, value, random_data) VALUES (?, ?, ?, ?)",
                                                 ConsistencyLevel.ALL, 1, i, "test 1-" + i, ByteBuffer.wrap(randomBytes));
                }

                // Flush to create second sstable
                cluster.get(1).nodetool("flush", "test");

                // Read entire partition before setting up backup memtable to verify data
                SimpleQueryResult result = cluster.coordinator(1).executeWithResult(
                    "SELECT id, sub_id, value FROM test.test_table WHERE id = 1", ConsistencyLevel.ALL);

                // Verify we have all 200 rows
                assertEquals("Expected 200 rows", 200, result.toObjectArrays().length);

                // Verify ordering - check first few and last few rows
                Object[][] rows = result.toObjectArrays();
                if (clusteringOrder.equals("DESC")) {
                    assertEquals(1, rows[0][0]);
                    assertEquals(199, rows[0][1]);
                    assertEquals("test 1-199", rows[0][2]);

                    assertEquals(1, rows[1][0]);
                    assertEquals(198, rows[1][1]);
                    assertEquals("test 1-198", rows[1][2]);

                    assertEquals(1, rows[199][0]);
                    assertEquals(0, rows[199][1]);
                    assertEquals("test 1-0", rows[199][2]);
                } else {
                    assertEquals(1, rows[0][0]);
                    assertEquals(0, rows[0][1]);
                    assertEquals("test 1-0", rows[0][2]);

                    assertEquals(1, rows[1][0]);
                    assertEquals(1, rows[1][1]);
                    assertEquals("test 1-1", rows[1][2]);

                    assertEquals(1, rows[199][0]);
                    assertEquals(199, rows[199][1]);
                    assertEquals("test 1-199", rows[199][2]);
                }

                // Setup backup memtable
                setupBackupMemtable(cluster);

                // Read entire partition from backup memtable (should merge data from both sstables across multiple chunks)
                result = cluster.coordinator(1).executeWithResult(
                    "SELECT id, sub_id, value FROM test.test_table WHERE id = 1", ConsistencyLevel.ALL);

                // Verify we still have all 200 rows
                assertEquals("Expected 200 rows from backup memtable", 200, result.toObjectArrays().length);

                // Verify ordering is still correct
                rows = result.toObjectArrays();
                if (clusteringOrder.equals("DESC")) {
                    assertEquals(1, rows[0][0]);
                    assertEquals(199, rows[0][1]);
                    assertEquals("test 1-199", rows[0][2]);

                    assertEquals(1, rows[1][0]);
                    assertEquals(198, rows[1][1]);
                    assertEquals("test 1-198", rows[1][2]);

                    assertEquals(1, rows[199][0]);
                    assertEquals(0, rows[199][1]);
                    assertEquals("test 1-0", rows[199][2]);
                } else {
                    assertEquals(1, rows[0][0]);
                    assertEquals(0, rows[0][1]);
                    assertEquals("test 1-0", rows[0][2]);

                    assertEquals(1, rows[1][0]);
                    assertEquals(1, rows[1][1]);
                    assertEquals("test 1-1", rows[1][2]);

                    assertEquals(1, rows[199][0]);
                    assertEquals(199, rows[199][1]);
                    assertEquals("test 1-199", rows[199][2]);
                }

                // Test range queries across the interleaved data spanning multiple chunks
                result = cluster.coordinator(1).executeWithResult(
                    "SELECT id, sub_id, value FROM test.test_table WHERE id = 1 AND sub_id >= 50 AND sub_id <= 60",
                    ConsistencyLevel.ALL);

                // Should have 11 rows (50-60 inclusive)
                assertEquals("Expected 11 rows in range", 11, result.toObjectArrays().length);

                rows = result.toObjectArrays();
                if (clusteringOrder.equals("DESC")) {
                    // DESC: 60, 59, 58, ..., 50
                    for (int i = 0; i < 11; i++) {
                        assertEquals(1, rows[i][0]);
                        assertEquals(60 - i, rows[i][1]);
                        assertEquals("test 1-" + (60 - i), rows[i][2]);
                    }
                } else {
                    // ASC: 50, 51, 52, ..., 60
                    for (int i = 0; i < 11; i++) {
                        assertEquals(1, rows[i][0]);
                        assertEquals(50 + i, rows[i][1]);
                        assertEquals("test 1-" + (50 + i), rows[i][2]);
                    }
                }
            }
        }
    }

    @Test
    public void testBackupMemtable_randomInterleavedSSTables() throws IOException
    {
        final long SEED = 12345L; // Configurable seed for reproducibility
        final int NUM_SSTABLES = 5;

        for(String clusteringOrder : CLUSTERING_ORDERS)
        {
            try (Cluster cluster = init(Cluster.build(1)
                                               .withDataDirCount(1)
                                               .start()))
            {
                // Create table with small compression chunk size and additional column for random data
                cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
                cluster.schemaChange("CREATE TABLE IF NOT EXISTS test.test_table (id int, sub_id int, value text, random_data blob, PRIMARY KEY (id, sub_id)) " +
                                   "WITH CLUSTERING ORDER BY (sub_id " + clusteringOrder + ") " +
                                   "AND compression = {'chunk_length_in_kb': '1', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");

                Random placementRandom = new Random(SEED);
                Random dataRandom = new Random(42);

                // Randomly assign each row (0-199) to one of 5 sstables
                List<List<Integer>> sstableRows = new ArrayList<>();
                for (int i = 0; i < NUM_SSTABLES; i++)
                {
                    sstableRows.add(new ArrayList<>());
                }

                for (int i = 0; i < 5000; i++)
                {
                    int sstableIndex = placementRandom.nextInt(NUM_SSTABLES);
                    sstableRows.get(sstableIndex).add(i);
                }

                // Write rows to each sstable
                for (int sstableIndex = 0; sstableIndex < NUM_SSTABLES; sstableIndex++)
                {
                    List<Integer> rowsForThisSSTable = sstableRows.get(sstableIndex);

                    for (int subId : rowsForThisSSTable)
                    {
                        byte[] randomBytes = new byte[256];
                        dataRandom.nextBytes(randomBytes);
                        cluster.coordinator(1).execute("INSERT INTO test.test_table (id, sub_id, value, random_data) VALUES (?, ?, ?, ?)",
                                                     ConsistencyLevel.ALL, 1, subId, "test 1-" + subId, ByteBuffer.wrap(randomBytes));
                    }

                    // Flush to create sstable
                    cluster.get(1).nodetool("flush", "test");
                }

                // Read entire partition before setting up backup memtable to verify data
                SimpleQueryResult result = cluster.coordinator(1).executeWithResult(
                    "SELECT id, sub_id, value FROM test.test_table WHERE id = 1", ConsistencyLevel.ALL);

                // Verify we have all 200 rows
                assertEquals("Expected 5000 rows", 5000, result.toObjectArrays().length);

                // Verify ordering - all rows should be in sorted order by sub_id
                Object[][] rows = result.toObjectArrays();
                for (int i = 0; i < 5000; i++)
                {
                    assertEquals(1, rows[i][0]);
                    if (clusteringOrder.equals("DESC")) {
                        assertEquals(4999 - i, rows[i][1]);
                        assertEquals("test 1-" + (4999 - i), rows[i][2]);
                    } else {
                        assertEquals(i, rows[i][1]);
                        assertEquals("test 1-" + i, rows[i][2]);
                    }
                }

                // Setup backup memtable
                setupBackupMemtable(cluster);

                // Read entire partition from backup memtable (should merge data from 5 sstables across multiple chunks)
                result = cluster.coordinator(1).executeWithResult(
                    "SELECT id, sub_id, value FROM test.test_table WHERE id = 1", ConsistencyLevel.ALL);

                // Verify we still have all 200 rows
                assertEquals("Expected 5000 rows from backup memtable", 5000, result.toObjectArrays().length);

                // Verify ordering is still correct after merging from S3
                rows = result.toObjectArrays();
                for (int i = 0; i < 5000; i++)
                {
                    assertEquals(1, rows[i][0]);
                    if (clusteringOrder.equals("DESC")) {
                        assertEquals(4999 - i, rows[i][1]);
                        assertEquals("test 1-" + (4999 - i), rows[i][2]);
                    } else {
                        assertEquals(i, rows[i][1]);
                        assertEquals("test 1-" + i, rows[i][2]);
                    }
                }

                // Test range queries across the randomly interleaved data spanning multiple chunks
                result = cluster.coordinator(1).executeWithResult(
                    "SELECT id, sub_id, value FROM test.test_table WHERE id = 1 AND sub_id >= 50 AND sub_id <= 60",
                    ConsistencyLevel.ALL);

                // Should have 11 rows (50-60 inclusive)
                assertEquals("Expected 11 rows in range", 11, result.toObjectArrays().length);

                rows = result.toObjectArrays();
                for (int i = 0; i < 11; i++)
                {
                    assertEquals(1, rows[i][0]);
                    if (clusteringOrder.equals("DESC")) {
                        assertEquals(60 - i, rows[i][1]);
                        assertEquals("test 1-" + (60 - i), rows[i][2]);
                    } else {
                        assertEquals(50 + i, rows[i][1]);
                        assertEquals("test 1-" + (50 + i), rows[i][2]);
                    }
                }
            }
        }
    }
}