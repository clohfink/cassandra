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
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.*;

/**
 * Tests for resource exhaustion scenarios during remote SSTable import.
 *
 * This test class covers:
 * - Disk space exhaustion during download/extraction
 * - HTTP retry exhaustion with failing servers
 * - Large file imports with memory monitoring
 */
public class RemoteImportResourceTest extends RemoteImportTestBase
{
    /**
     * Static nested class to fake full disk for testing.
     * Must be static to avoid capturing test instance (which isn't serializable).
     */
    public static class FullDiskUsageMonitor extends DiskUsageMonitor
    {
        @Override
        public double getDiskUsage()
        {
            return 0.96; // Fake 96% disk usage - above fail threshold
        }
    }
    /**
     * Test that import handles disk space exhaustion gracefully.
     * This verifies that the system detects low disk space and fails gracefully
     * rather than filling the disk completely.
     */
    @Test
    public void testDiskSpaceExhaustion() throws Exception
    {
        SSTableConfig config = createSSTableConfig("testk_disk_", "org.apache.cassandra.dht.Murmur3Partitioner", 1000, false);
        SSTableZipResult zipResult = createSSTableZipWithConfig(config, false);
        tempSSTableZip = zipResult.zipPath;

        try (Cluster cluster = init(Cluster.build(2)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 1)
                                                              // Enable disk usage guardrail with low threshold
                                                              .set("data_disk_usage_percentage_warn_threshold", 80)
                                                              .set("data_disk_usage_percentage_fail_threshold", 95))
                                            .start()))
        {
            setupHttpServer();
            String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);

            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");

            // Override DiskUsageMonitor on each node to fake full disk (96% usage)
            cluster.get(1).runOnInstance(() -> DiskUsageMonitor.instance = new FullDiskUsageMonitor());
            cluster.get(2).runOnInstance(() -> DiskUsageMonitor.instance = new FullDiskUsageMonitor());

            UUID importId = UUID.randomUUID();
            startImportJob(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl, zipResult.startToken, zipResult.endToken);

            // Wait for import to detect disk space issue and fail
            String finalStatus = waitForImportCompletionWithAutoTransition(cluster, importId, TEST_KEYSPACE, TEST_TABLE, 60);
            assertEquals("Import should fail with ERROR status due to disk space", "ERROR", finalStatus);

            // Verify the system didn't crash and cleanup occurred
            verifyCleanupOccurred(cluster, importId);
        }
    }

    /**
     * Test HTTP retry exhaustion when server consistently returns errors.
     * Verifies that the import eventually fails after exhausting retries
     * and that retry metrics are correctly incremented.
     */
    @Test
    public void testHttpRetryExhaustion() throws Exception
    {
        SSTableConfig config = createSSTableConfig("testk_retry_", "org.apache.cassandra.dht.Murmur3Partitioner", 50, false);
        SSTableZipResult zipResult = createSSTableZipWithConfig(config, false);
        tempSSTableZip = zipResult.zipPath;

        // Create a failing HTTP server that always returns 500
        AtomicInteger requestCount = new AtomicInteger(0);
        HttpServer failingServer = HttpServer.create(new InetSocketAddress(serverPort), 0);
        failingServer.createContext("/sstable.zip", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                int count = requestCount.incrementAndGet();
                logger.info("Failing server received request #{}", count);
                exchange.sendResponseHeaders(500, -1);
                exchange.close();
            }
        });
        failingServer.start();
        httpServer = failingServer;

        try (Cluster cluster = init(Cluster.build(2)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 1)
                                                              .set("import_http_retry_max_attempts", 3)
                                                              .set("import_http_retry_initial_delay", "100ms"))
                                            .start()))
        {
            String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);

            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");

            UUID importId = UUID.randomUUID();
            startImportJob(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl, zipResult.startToken, zipResult.endToken);

            // Wait for import to reach ERROR state due to retry exhaustion
            String finalStatus = waitForImportCompletionWithAutoTransition(cluster, importId, TEST_KEYSPACE, TEST_TABLE, 60);
            assertEquals("Import should fail with ERROR status due to HTTP errors", "ERROR", finalStatus);

            // Verify that multiple retry attempts were made
            assertTrue("Should have made multiple retry attempts", requestCount.get() > 1);
            logger.info("HTTP server received {} total requests before giving up", requestCount.get());

            // Verify no partial data was imported
            verifyNoDataImported(cluster);
        }
    }

    // Helper methods

    private void verifyNoDataImported(Cluster cluster) throws Exception
    {
        long count = getRowCount(cluster, TEST_KEYSPACE, TEST_TABLE);
        assertEquals("Should not have imported any data after failure", 0, count);
    }

    private void verifyCleanupOccurred(Cluster cluster, UUID importId)
    {
        // Verify that staging directories were cleaned up
        // Staging is in table data dir: <table_data_dir>/imports/<job_id>/
        cluster.get(1).runOnInstance(() -> {
            try
            {
                TableMetadata metadata = Schema.instance.getTableMetadata(TEST_KEYSPACE, TEST_TABLE);
                if (metadata != null)
                {
                    Directories dirs = new Directories(metadata);
                    File base = dirs.getCFDirectories().get(0);
                    File importsDir = new File(base, "imports");

                    if (importsDir.exists())
                    {
                        File[] importDirs = importsDir.tryList();
                        if (importDirs != null)
                        {
                            for (File dir : importDirs)
                            {
                                if (dir.name().equals(importId.toString()))
                                {
                                    Assert.fail("Staging directory should have been cleaned up: " + dir.absolutePath());
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                logger.warn("Failed to verify cleanup", e);
            }
        });
    }
}
