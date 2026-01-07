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

import java.nio.file.Files;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for remote import replication behavior across multiple nodes.
 */
public class RemoteImportReplicationTest extends RemoteImportTestBase
{
    @Test
    public void testTwoNodeReplicationVerification() throws Throwable {
        SSTableZipResult zipResult = createSSTableZip();
        tempSSTableZip = zipResult.zipPath;
        setupHttpServer();

        String localUrl = "http://localhost:" + serverPort + "/sstable.zip";

        try (Cluster cluster = setupTestCluster(2, 2)) {
            runFullImportTest(UUID.randomUUID(), TEST_KEYSPACE, TEST_TABLE, localUrl, zipResult.startToken, zipResult.endToken, Files.size(tempSSTableZip), 0);

            // Verify data on both nodes using executeInternal
            for (int nodeId = 1; nodeId <= 2; nodeId++) {
                Object[][] result = cluster.get(nodeId).executeInternal("SELECT COUNT(*) FROM " + TEST_KEYSPACE + '.' + TEST_TABLE);
                long count = (Long) result[0][0];
                logger.info("Node {} has {} rows", nodeId, count);
                assertEquals("Node " + nodeId + " should have all 1000 rows", 1000L, count);

                Object[][] dataResult = cluster.get(nodeId).executeInternal("SELECT id, data FROM " + TEST_KEYSPACE + '.' + TEST_TABLE + " LIMIT 5");
                assertTrue("Node " + nodeId + " should have at least 5 rows of data", dataResult.length >= 5);

                boolean foundExpectedData = false;
                for (Object[] row : dataResult) {
                    String data = (String) row[1];
                    if (data != null && data.startsWith("test data ")) {
                        foundExpectedData = true;
                        break;
                    }
                }
                assertTrue("Node " + nodeId + " should have expected test data format", foundExpectedData);
            }

            logger.info("Two-node replication verification completed successfully - both nodes have all 1000 imported rows");
        }
    }

    @Test
    public void testTwoNodeNonReplicationVerification() throws Throwable {
        // Create SSTable with just 1 row to make verification simple
        SSTableZipResult zipResult = createSSTableZipSinglePartition();
        tempSSTableZip = zipResult.zipPath;
        setupHttpServer();

        String localUrl = "http://localhost:" + serverPort + "/sstable.zip";

        try (Cluster cluster = setupTestCluster(2, 1)) { // RF=1 so data only goes to primary replica
            runFullImportTest(UUID.randomUUID(), TEST_KEYSPACE, TEST_TABLE, localUrl, zipResult.startToken, zipResult.endToken, Files.size(tempSSTableZip), 1);

            // Verify that only one node has the single partition
            Object[][] node1Result = cluster.get(1).executeInternal("SELECT COUNT(*) FROM " + TEST_KEYSPACE + '.' + TEST_TABLE);
            Object[][] node2Result = cluster.get(2).executeInternal("SELECT COUNT(*) FROM " + TEST_KEYSPACE + '.' + TEST_TABLE);

            long node1Count = (Long) node1Result[0][0];
            long node2Count = (Long) node2Result[0][0];

            logger.info("Node 1 has {} rows, Node 2 has {} rows", node1Count, node2Count);

            // With RF=1 and only 1 partition, exactly one node should have the data
            long totalRows = node1Count + node2Count;
            assertEquals("Total rows should be exactly 1", 1L, totalRows);

            // Exactly one node should have the data, the other should have zero
            assertTrue("Exactly one node should have data",
                      (node1Count == 1 && node2Count == 0) || (node1Count == 0 && node2Count == 1));

            if (node1Count == 1) {
                logger.info("Node 1 has the single partition, Node 2 correctly has none - RF=1 working properly");
            } else {
                logger.info("Node 2 has the single partition, Node 1 correctly has none - RF=1 working properly");
            }
        }
    }
}
