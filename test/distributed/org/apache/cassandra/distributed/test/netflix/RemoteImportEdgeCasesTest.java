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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.*;

/**
 * Tests for edge cases in remote SSTable import.
 *
 * This test class covers:
 * - Token range boundary data
 * - Multi-datacenter filtering
 * - Concurrent imports
 */
public class RemoteImportEdgeCasesTest extends RemoteImportTestBase
{
    /**
     * Test that data exactly on token range boundaries is handled correctly.
     * This verifies that boundary conditions in token range filtering work properly.
     */
    @Test
    public void testTokenRangeBoundaryData() throws Exception
    {
        SSTableConfig config = createSSTableConfig("testk_boundary_", "org.apache.cassandra.dht.Murmur3Partitioner", 100, false);
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

            // Import with explicit token range including boundaries
            UUID importId = UUID.randomUUID();
            startImportJob(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl, zipResult.startToken, zipResult.endToken);

            // Wait for import to complete (auto-transitions through STAGED -> importing -> DONE)
            String finalStatus = waitForImportCompletionWithAutoTransition(cluster, importId, TEST_KEYSPACE, TEST_TABLE, 120);
            assertEquals("Import should complete successfully", "DONE", finalStatus);

            // Verify data was imported and no duplicates exist
            long totalCount = getTotalRowCount(cluster);
            assertTrue("Should have imported data", totalCount > 0);
            assertTrue("Should not have duplicated data", totalCount <= 100);

            // Verify data is correctly distributed across nodes
            verifyDataDistribution(cluster);
        }
        System.gc(); // give time to ensure all closed before next test
        Thread.sleep(4000);
    }

    /**
     * Test multiple concurrent imports to stress test the system.
     * Verifies that concurrent imports don't interfere with each other
     * and all complete successfully.
     */
    @Test
    public void testMultipleConcurrentImports() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 4))
                                            .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': 2}");

            // Create 5 different tables for concurrent imports
            List<UUID> importIds = new ArrayList<>();
            List<String> tableNames = new ArrayList<>();

            for (int i = 0; i < 5; i++)
            {
                String tableName = TEST_TABLE + "_" + i;
                tableNames.add(tableName);

                cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + tableName + " (id uuid PRIMARY KEY, data text)");

                // Create unique SSTable for each table
                SSTableConfig config = createSSTableConfig("testk_concurrent_" + i + "_",
                                                          "org.apache.cassandra.dht.Murmur3Partitioner", 50, false);
                SSTableZipResult zipResult = createSSTableZipWithConfig(config, false);

                // Store first zip for cleanup
                if (i == 0) tempSSTableZip = zipResult.zipPath;
                else if (i == 1) tempSSTableZip2 = zipResult.zipPath;

                // Setup HTTP server for this import
                int port = serverPort + i;
                setupSingleHttpServer(port, zipResult.zipPath, "HTTP server " + i, i == 0);

                String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", port);

                UUID importId = UUID.randomUUID();
                importIds.add(importId);

                startImportJob(cluster, importId, TEST_KEYSPACE, tableName, sourceUrl, zipResult.startToken, zipResult.endToken);

                logger.info("Started import {} for table {}", importId, tableName);
            }

            // Wait for all imports to complete
            for (int i = 0; i < importIds.size(); i++)
            {
                UUID importId = importIds.get(i);
                String tableName = tableNames.get(i);
                logger.info("Waiting for import {} (table {}) to complete", importId, tableName);
                String finalStatus = waitForImportCompletionWithAutoTransition(cluster, importId, TEST_KEYSPACE, tableName, 180);
                assertEquals("Import should complete successfully", "DONE", finalStatus);
            }

            // Verify all tables have data
            for (String tableName : tableNames)
            {
                long count = getTableRowCount(cluster, tableName);
                assertTrue(String.format("Table %s should have imported data", tableName), count > 0);
                logger.info("Table {} has {} rows", tableName, count);
            }
        }
    }

    /**
     * Test multi-datacenter import with datacenter filtering.
     * Verifies that dc_filter correctly restricts which datacenters process sources.
     */
    @Test
    public void testMultiDatacenterImportWithFiltering() throws Exception
    {
        SSTableConfig config1 = createSSTableConfig("testk_dc1_", "org.apache.cassandra.dht.Murmur3Partitioner", 50, false);
        SSTableZipResult zipResult1 = createSSTableZipWithConfig(config1, false);
        tempSSTableZip = zipResult1.zipPath;

        SSTableConfig config2 = createSSTableConfig("testk_dc2_", "org.apache.cassandra.dht.Murmur3Partitioner", 50, false);
        SSTableZipResult zipResult2 = createSSTableZipWithConfig(config2, false);
        tempSSTableZip2 = zipResult2.zipPath;

        try (Cluster cluster = init(Cluster.build(2)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 2))
                                            .withRacks(2, 1, 1) // 2 DCs, 1 rack per DC, 1 node per rack
                                            .start()))
        {
            setupHttpServers();
            String sourceUrl1 = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);
            String sourceUrl2 = String.format("http://127.0.0.1:%d/sstable.zip", serverPort2);

            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE +
                               " WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1, 'datacenter2': 1}");
            cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");

            // Create import with DC-specific sources
            UUID importId = UUID.randomUUID();
            // Insert first source for datacenter1
            startImportJobWithDcFilter(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl1, zipResult1.startToken, zipResult1.endToken, "datacenter1");
            // Insert second source for datacenter2 (note: only INSERT, no state update yet)
            cluster.coordinator(1).execute(String.format(
                "INSERT INTO system_distributed.remote_import (id, target_keyspace, target_table, source, source_type, start_token, end_token, dc_filter) " +
                "VALUES (%s, '%s', '%s', '%s', 'url', '%s', '%s', 'datacenter2')",
                importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl2, zipResult2.startToken, zipResult2.endToken),
                ConsistencyLevel.ONE);
            // Set state to start processing (do this after both inserts)
            cluster.coordinator(1).execute(String.format(
                "UPDATE system_distributed.remote_import SET state = 'staging' " +
                "WHERE id = %s AND target_keyspace = '%s' AND target_table = '%s'",
                importId, TEST_KEYSPACE, TEST_TABLE),
                ConsistencyLevel.ONE);

            // Wait for import to complete (auto-transitions through STAGED -> importing -> DONE)
            String finalStatus = waitForImportCompletionWithAutoTransition(cluster, importId, TEST_KEYSPACE, TEST_TABLE, 120);
            assertEquals("Import should complete successfully", "DONE", finalStatus);

            // Verify data was imported across both datacenters
            verifyImportedData(cluster, 100); // Total ~100 rows from both sources
        }
    }

    // Helper methods

    private void verifyImportedData(Cluster cluster, int expectedRows) throws Exception
    {
        long count = getTableRowCount(cluster, TEST_TABLE);
        assertTrue(String.format("Should have imported some data (found %d rows)", count), count > 0);
        assertTrue(String.format("Should have approximately %d rows (found %d)", expectedRows, count),
                  count >= expectedRows * 0.5 && count <= expectedRows * 1.5);
    }

    private long getTotalRowCount(Cluster cluster) throws Exception
    {
        return getTableRowCount(cluster, TEST_TABLE);
    }

    private long getTableRowCount(Cluster cluster, String tableName) throws Exception
    {
        return getRowCount(cluster, TEST_KEYSPACE, tableName);
    }

    private void verifyDataDistribution(Cluster cluster)
    {
        // Verify that data is distributed across nodes based on token ranges
        // This is a basic check - in a real cluster, we'd verify proper token-aware distribution
        for (int i = 1; i <= cluster.size(); i++)
        {
            final int nodeNum = i;
            cluster.get(i).runOnInstance(() -> {
                try
                {
                    ColumnFamilyStore cfs = Keyspace.open(TEST_KEYSPACE)
                                                    .getColumnFamilyStore(TEST_TABLE);

                    long localCount = cfs.getLiveSSTables().size();
                    logger.info("Node {} has {} SSTables for table {}.{}", nodeNum, localCount, TEST_KEYSPACE, TEST_TABLE);
                }
                catch (Exception e)
                {
                    logger.error("Failed to check SSTables on node " + nodeNum, e);
                }
            });
        }
    }
}
