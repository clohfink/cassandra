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

import java.time.Instant;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.SimpleQueryResult;

public class BackupMemtableQueryTest extends BackupMemtableTestBase
{
    @Test
    public void testBackupMemtable_QueryOperations() throws Exception
    {
        for(String clusteringOrder : CLUSTERING_ORDERS)
        {
            try (Cluster cluster = init(Cluster.build(1)
                                               .withDataDirCount(1)
                                               .start()))
            {
                setupTable(cluster, clusteringOrder);
                insertTestData(cluster);
                setupBackupMemtable(cluster);

                // Test single value query
                SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM test.test_table WHERE id = 93 AND sub_id = 1", ConsistencyLevel.ALL);
                assertQueryResults(result.toObjectArrays(),
                                   new Object[][]{ { 93, 1, "test 93-1" } });

                // Test entire partition query
                result = cluster.coordinator(1).executeWithResult("SELECT * FROM test.test_table WHERE id = 50", ConsistencyLevel.ALL);
                if (clusteringOrder.equals("DESC")) {
                    assertQueryResults(result.toObjectArrays(),
                                       new Object[][]{ { 50, 4, "test 50-4" },
                                                       { 50, 3, "test 50-3" },
                                                       { 50, 2, "test 50-2" },
                                                       { 50, 1, "test 50-1" },
                                                       { 50, 0, "test 50-0" } });
                } else {
                    assertQueryResults(result.toObjectArrays(),
                                       new Object[][]{ { 50, 0, "test 50-0" }, { 50, 1, "test 50-1" }, { 50, 2, "test 50-2" }, { 50, 3, "test 50-3" }, { 50, 4, "test 50-4" } });
                }

                // Test all partitions extensively
                for(int i = 0; i < 100; i++)
                {
                    result = cluster.coordinator(1).executeWithResult("SELECT * FROM test.test_table WHERE id = ?", ConsistencyLevel.ALL, i);
                    if (clusteringOrder.equals("DESC")) {
                        assertQueryResults(result.toObjectArrays(),
                                           new Object[][]{ { i, 4, "test " + i + "-4" },
                                                           { i, 3, "test " + i + "-3" },
                                                           { i, 2, "test " + i + "-2" },
                                                           { i, 1, "test " + i + "-1" },
                                                           { i, 0, "test " + i + "-0" } });
                    } else {
                        assertQueryResults(result.toObjectArrays(),
                                           new Object[][]{ { i, 0, "test " + i + "-0" },
                                                           { i, 1, "test " + i + "-1" },
                                                           { i, 2, "test " + i + "-2" },
                                                           { i, 3, "test " + i + "-3" },
                                                           { i, 4, "test " + i + "-4" } });
                    }
                }
            }
        }
    }

    @Test
    public void testBackupMemtable_FilteredQueries() throws Exception
    {
        for(String clusteringOrder : CLUSTERING_ORDERS)
        {
            try (Cluster cluster = init(Cluster.build(1)
                                               .withDataDirCount(1)
                                               .start()))
            {
                setupTable(cluster, clusteringOrder);
                insertTestData(cluster);

                // Test before S3 setup
                SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT sub_id, value FROM test.test_table WHERE id = 33 AND sub_id >= 1 AND sub_id <= 2", ConsistencyLevel.ALL);
                Object[][] expectedBefore = clusteringOrder.equals("DESC") ?
                    new Object[][]{ { 2, "test 33-2" }, { 1, "test 33-1" } } :
                    new Object[][]{ { 1, "test 33-1" }, { 2, "test 33-2" } };
                assertQueryResults(result.toObjectArrays(), expectedBefore);

                setupBackupMemtable(cluster);

                // Test cluster column filter after S3 setup
                result = cluster.coordinator(1).executeWithResult("SELECT sub_id, value FROM test.test_table WHERE id = 33 AND sub_id >= 1 AND sub_id <= 2", ConsistencyLevel.ALL);
                assertQueryResults(result.toObjectArrays(), expectedBefore);

                // Test limit query with filter
                result = cluster.coordinator(1).executeWithResult("SELECT sub_id, value FROM test.test_table WHERE id = 33 AND sub_id >= 2 LIMIT 3", ConsistencyLevel.ALL);
                Object[][] expectedLimit = clusteringOrder.equals("DESC") ?
                    new Object[][]{ { 4, "test 33-4" }, { 3, "test 33-3" }, { 2, "test 33-2" } } :
                    new Object[][]{ { 2, "test 33-2" }, { 3, "test 33-3" }, { 4, "test 33-4" } };
                assertQueryResults(result.toObjectArrays(), expectedLimit);
            }
        }
    }

    @Test
    public void testBackupMemtableQuery_multiColumnClustering_ASC_DESC_variableDataTypes() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withDataDirCount(1)
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE test.test_table (customer_id uuid, order_ts bigint, order_id uuid, total int, status text, PRIMARY KEY (customer_id, order_ts, order_id)) WITH CLUSTERING ORDER BY (order_ts DESC, order_id ASC)");

            String insertStatement = "INSERT INTO test.test_table (customer_id, order_ts, order_id, total, status) VALUES (?, ?, ?, ?, ?)";
            cluster.coordinator(1).execute(insertStatement, ConsistencyLevel.ALL,
                                           UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                                           Instant.parse("2024-06-01T12:00:00Z").toEpochMilli(),
                                           UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                                           100,
                                           "shipped");
            cluster.coordinator(1).execute(insertStatement, ConsistencyLevel.ALL,
                                           UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                                           Instant.parse("2024-06-02T13:00:00Z").toEpochMilli(),
                                           UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"),
                                           50,
                                           "pending");
            cluster.coordinator(1).execute(insertStatement, ConsistencyLevel.ALL,
                                           UUID.fromString("dddddddd-dddd-dddd-dddd-dddddddddddd"),
                                           Instant.parse("2024-06-01T14:00:00Z").toEpochMilli(),
                                           UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"),
                                           200,
                                           "delivered");
            cluster.get(1).nodetool("flush", "test");

            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM test.test_table WHERE customer_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa AND order_ts >= ?", ConsistencyLevel.ALL, Instant.parse("2024-06-02T00:00:00Z").toEpochMilli());
            assertQueryResults(result.toObjectArrays(),
                               new Object[][]{ { UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), 1717333200000L, UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"), "pending", 50 } });
            setupBackupMemtable(cluster);

            // Query backup memtable.
            result = cluster.coordinator(1).executeWithResult("SELECT * FROM test.test_table WHERE customer_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa AND order_ts >= ?", ConsistencyLevel.ALL, Instant.parse("2024-06-02T00:00:00Z").toEpochMilli());
            assertQueryResults(result.toObjectArrays(),
                               new Object[][]{ { UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), 1717333200000L, UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc"), "pending", 50 } });
        }
    }
}