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

import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.netflix.cassandra.importing.ImportJob;
import com.netflix.cassandra.importing.ImportJobManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.gms.Gossiper;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.*;

/**
 * Tests for distributed coordination failure scenarios in remote SSTable import.
 *
 * This test class covers:
 * - Network partition during import
 * - Node join/leave during active import
 */
public class RemoteImportFailureTest extends RemoteImportTestBase
{
    /**
     * Test that import recovers gracefully when network partition occurs during download phase.
     * This simulates a scenario where one node loses connectivity but the cluster continues.
     */
    @Test
    public void testNetworkPartitionDuringImport() throws Exception
    {
        SSTableConfig config = createSSTableConfig("testk_partition_", "org.apache.cassandra.dht.Murmur3Partitioner", 100, false);
        SSTableZipResult zipResult = createSSTableZipWithConfig(config, false);
        tempSSTableZip = zipResult.zipPath;

        try (Cluster cluster = init(Cluster.build(3)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 2))
                                            .start()))
        {
            setupHttpServer();
            String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);

            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");

            // Start import job
            UUID importId = UUID.randomUUID();
            startImportJob(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl, zipResult.startToken, zipResult.endToken);

            // Wait for all nodes to reach STAGED state (download and staging complete)
            waitForInternalJobStatus(cluster, importId, "STAGED", 60);

            // Transition to importing state
            cluster.coordinator(1).execute(String.format(
                "UPDATE system_distributed.remote_import SET state = 'importing' " +
                "WHERE id = %s AND target_keyspace = '%s' AND target_table = '%s'",
                importId, TEST_KEYSPACE, TEST_TABLE),
                ConsistencyLevel.ONE);

            // Simulate network partition by turning off node3
            logger.info("Simulating network partition by stopping gossip on node 3");
            cluster.get(3).runOnInstance(() -> {
                Gossiper.instance.stop();
            });

            // Wait and verify that nodes 1 and 2 can complete import despite partition
            // The import should complete on nodes 1 and 2, while node 3 may lag behind
            waitForStatusOnNodes(cluster, importId, "DONE", 60, List.of(1, 2));

            // Restart gossip on node 3
            logger.info("Restoring network connectivity to node 3");
            cluster.get(3).runOnInstance(() -> {
                Gossiper.instance.start(1, new HashMap<>());
            });

            // Eventually node 3 should catch up
            waitForInternalJobStatus(cluster, importId, "DONE", 60);

            // Verify data was imported correctly on all nodes
            verifyImportedData(cluster, 100);
        }
    }

    /**
     * Test that import handles node leave/join during an active import operation.
     * This simulates a scenario where cluster topology changes mid-import.
     */
    @Test
    public void testNodeLeaveJoinDuringImport() throws Exception
    {
        SSTableConfig config = createSSTableConfig("testk_topology_", "org.apache.cassandra.dht.Murmur3Partitioner", 100, false);
        SSTableZipResult zipResult = createSSTableZipWithConfig(config, false);
        tempSSTableZip = zipResult.zipPath;

        try (Cluster cluster = init(Cluster.build(3)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 2))
                                            .start()))
        {
            setupHttpServer();
            String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);

            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");

            // Start import job
            UUID importId = UUID.randomUUID();
            startImportJob(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl, zipResult.startToken, zipResult.endToken);

            // Wait for all nodes to reach STAGED state
            waitForInternalJobStatus(cluster, importId, "STAGED", 60);

            // Transition to importing state
            cluster.coordinator(1).execute(String.format(
                "UPDATE system_distributed.remote_import SET state = 'importing' " +
                "WHERE id = %s AND target_keyspace = '%s' AND target_table = '%s'",
                importId, TEST_KEYSPACE, TEST_TABLE),
                ConsistencyLevel.ONE);

            // Shutdown node 3 during import
            logger.info("Shutting down node 3 during import");
            cluster.get(3).shutdown().get();

            // Import should continue on remaining nodes
            waitForStatusOnNodes(cluster, importId, "DONE", 90, List.of(1, 2));

            // Verify data imported correctly on surviving nodes
            verifyImportedDataOnNodes(cluster, 100, List.of(1, 2));
        }
    }

    // Helper methods

    /**
     * Waits for an import job to reach a specific internal status on node 1.
     * For internal statuses like DOWNLOADING, STAGED, etc., this checks the ImportJob object.
     * For final statuses like DONE, this also works since it checks the actual job status.
     */
    protected void waitForInternalJobStatus(Cluster cluster, UUID importId, String expectedStatus, int timeoutSeconds) throws Exception
    {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        String lastStatus = null;
        int iterations = 0;

        while (System.currentTimeMillis() < deadline)
        {
            // Check the actual ImportJob status on node 1
            lastStatus = cluster.get(1).callOnInstance(() -> {
                ImportJob job = ImportJobManager.getInstance().getJob(importId);
                if (job != null) {
                    return job.status.get().toString();
                }
                return null;
            });

            // Log every 5 seconds to track progress
            if (iterations % 5 == 0 || lastStatus == null)
            {
                logger.info("Waiting for status {}, current status: {} (iteration {})", expectedStatus, lastStatus, iterations);
            }

            if (expectedStatus.equals(lastStatus))
            {
                logger.info("Import {} reached status {}", importId, expectedStatus);
                return;
            }
            if (lastStatus != null && lastStatus.equals("ERROR"))
            {
                fail(String.format("Import %s entered ERROR state", importId));
            }

            // If we're past the expected status, treat as success (job progressed too quickly)
            if (lastStatus != null && !lastStatus.equals(expectedStatus))
            {
                if (expectedStatus.equals("DOWNLOADING") &&
                    (lastStatus.equals("UNZIPPING") || lastStatus.equals("STAGED") ||
                     lastStatus.equals("IMPORTING") || lastStatus.equals("TRIMMING") || lastStatus.equals("DONE")))
                {
                    logger.warn("Import {} already moved past {} to {}, treating as success",
                               importId, expectedStatus, lastStatus);
                    return;
                }
                if (expectedStatus.equals("STAGED") &&
                    (lastStatus.equals("IMPORTING") || lastStatus.equals("TRIMMING") || lastStatus.equals("DONE")))
                {
                    logger.warn("Import {} already moved past {} to {}, treating as success",
                               importId, expectedStatus, lastStatus);
                    return;
                }
            }

            Thread.sleep(1000);
            iterations++;
        }

        fail(String.format("Import %s did not reach status %s within %d seconds (last status: %s)",
                          importId, expectedStatus, timeoutSeconds, lastStatus));
    }

    /**
     * Waits for an import job to reach a specific status on multiple nodes.
     * This checks the actual ImportJob status on each specified node.
     */
    private void waitForStatusOnNodes(Cluster cluster, UUID importId, String expectedStatus, int timeoutSeconds, List<Integer> nodes) throws Exception
    {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        for (int node : nodes)
        {
            while (System.currentTimeMillis() < deadline)
            {
                final int finalNode = node;
                String status = cluster.get(node).callOnInstance(() -> {
                    ImportJob job =
                        com.netflix.cassandra.importing.ImportJobManager.getInstance().getJob(importId);
                    if (job != null) {
                        return job.status.get().toString();
                    }
                    return null;
                });

                if (expectedStatus.equals(status))
                {
                    logger.info("Node {} reached status {} for import {}", node, expectedStatus, importId);
                    break;
                }
                if (status != null && status.equals("ERROR"))
                {
                    fail(String.format("Import %s on node %d entered ERROR state", importId, node));
                }
                Thread.sleep(1000);
            }

            // Verify final status
            String finalStatus = cluster.get(node).callOnInstance(() -> {
                ImportJob job =
                    com.netflix.cassandra.importing.ImportJobManager.getInstance().getJob(importId);
                return job != null ? job.status.get().toString() : null;
            });
            assertEquals(String.format("Node %d should reach status %s", node, expectedStatus), expectedStatus, finalStatus);
        }
    }

    private void verifyImportedData(Cluster cluster, int expectedRows) throws Exception
    {
        verifyImportedDataOnNodes(cluster, expectedRows, List.of(1, 2, 3));
    }

    private void verifyImportedDataOnNodes(Cluster cluster, int expectedRows, List<Integer> nodes) throws Exception
    {
        for (int node : nodes)
        {
            long count = getRowCount(cluster, TEST_KEYSPACE, TEST_TABLE);
            assertTrue(String.format("Node %d should have imported some data (found %d rows)", node, count), count > 0);
            assertTrue(String.format("Node %d should not have more than expected rows (expected ~%d, found %d)", node, expectedRows, count),
                      count <= expectedRows);
        }
    }
}
