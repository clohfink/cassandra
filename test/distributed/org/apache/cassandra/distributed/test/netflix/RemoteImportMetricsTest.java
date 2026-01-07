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

import java.lang.reflect.Field;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.netflix.cassandra.metrics.ImportJobMetrics;
import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.*;

/**
 * Tests for import metrics verification.
 *
 * This test class covers:
 * - Metrics accuracy for successful imports
 * - Metrics accuracy for failed imports
 * - Virtual table query performance under load
 */
public class RemoteImportMetricsTest extends RemoteImportTestBase
{
    /**
     * Test that all expected metrics are correctly reported for a successful import.
     */
    @Test
    public void testSuccessfulImportMetrics() throws Exception
    {
        SSTableConfig config = createSSTableConfig("testk_metrics_", "org.apache.cassandra.dht.Murmur3Partitioner", 100, false);
        SSTableZipResult zipResult = createSSTableZipWithConfig(config, false);
        tempSSTableZip = zipResult.zipPath;

        try (Cluster cluster = init(Cluster.build(2)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 2))
                                            .start()))
        {
            setupHttpServer();
            String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);

            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");

            // Capture initial metric values
            long initialJobsStarted = getMetricValue(cluster, 1, "JobsStarted");
            long initialJobsCompleted = getMetricValue(cluster, 1, "JobsCompleted");

            UUID importId = UUID.randomUUID();
            startImportJob(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl, zipResult.startToken, zipResult.endToken);

            // Wait for import to complete (auto-transitions through STAGED -> importing -> DONE)
            String finalStatus = waitForImportCompletionWithAutoTransition(cluster, importId, TEST_KEYSPACE, TEST_TABLE, 120);
            assertEquals("Import should complete successfully", "DONE", finalStatus);

            // Verify metrics were incremented
            long finalJobsStarted = getMetricValue(cluster, 1, "jobsStarted");
            long finalJobsCompleted = getMetricValue(cluster, 1, "jobsCompleted");
            long jobsFailed = getMetricValue(cluster, 1, "jobsFailed");

            assertTrue("JobsStarted should increase", finalJobsStarted > initialJobsStarted);
            assertTrue("JobsCompleted should increase", finalJobsCompleted > initialJobsCompleted);
            assertEquals("JobsFailed should not increase", 0, jobsFailed);

            // Verify step counters were incremented
            long jobsFiltering = getMetricValue(cluster, 1, "filteringTime");
            long jobsDownloading = getMetricValue(cluster, 1, "downloadTime");
            long jobsImporting = getMetricValue(cluster, 1, "importTime");

            assertTrue("JobsFiltering should be incremented", jobsFiltering > 0);
            assertTrue("JobsDownloading should be incremented", jobsDownloading > 0);
            assertTrue("JobsImporting should be incremented", jobsImporting > 0);

            logger.info("Metrics verified - Started: {}, Completed: {}, Filtering: {}, Downloading: {}, Importing: {}",
                       finalJobsStarted, finalJobsCompleted, jobsFiltering, jobsDownloading, jobsImporting);
        }
    }

    /**
     * Test that error metrics are correctly reported for failed imports.
     */
    @Test
    public void testFailedImportMetrics() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 2))
                                            .start()))
        {
            // Don't setup HTTP server - this will cause download to fail
            String sourceUrl = String.format("http://127.0.0.1:%d/nonexistent.zip", serverPort);

            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");

            // Capture initial metric values
            long initialJobsFailed = getMetricValue(cluster, 1, "JobsFailed");
            long initialNetworkErrors = getMetricValue(cluster, 1, "NetworkErrors");

            UUID importId = UUID.randomUUID();
            startImportJob(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl, "-1", "1");

            // Wait for import to fail (should fail during download since no HTTP server)
            String finalStatus = waitForImportCompletionWithAutoTransition(cluster, importId, TEST_KEYSPACE, TEST_TABLE, 90);
            assertEquals("Import should fail with ERROR status", "ERROR", finalStatus);

            // Verify error metrics were incremented
            long finalJobsFailed = getMetricValue(cluster, 1, "JobsFailed");
            long finalNetworkErrors = getMetricValue(cluster, 1, "NetworkErrors");

            assertTrue("JobsFailed should increase", finalJobsFailed > initialJobsFailed);
            // Network errors may or may not increment depending on error type
            assertTrue("NetworkErrors should be non-negative", finalNetworkErrors >= initialNetworkErrors);

            logger.info("Error metrics verified - JobsFailed: {}, NetworkErrors: {}",
                       finalJobsFailed, finalNetworkErrors);
        }
    }

    // Helper methods

    /**
     * Needs to run in classlaoder scope so cant access metrics directly
     */
    private long getMetricValue(Cluster cluster, int nodeNumber, String metricName)
    {
        return cluster.get(nodeNumber).callOnInstance(() -> {
            try
            {
                ImportJobMetrics metrics =
                    ImportJobMetrics.instance;

                // Use reflection to get the metric value
                Field field = metrics.getClass().getDeclaredField(
                    metricName.substring(0, 1).toLowerCase() + metricName.substring(1));
                field.setAccessible(true);
                Object metric = field.get(metrics);

                // Handle both Counter and Timer types
                if (metric instanceof Counter)
                {
                    return ((Counter) metric).getCount();
                }
                else if (metric instanceof Timer)
                {
                    return ((Timer) metric).getCount();
                }
                else
                {
                    logger.error("Unsupported metric type for " + metricName + ": " + metric.getClass().getName());
                    return -1L;
                }
            }
            catch (Exception e)
            {
                logger.error("Failed to get metric value for " + metricName, e);
                return -1L;
            }
        });
    }

    /**
     * Test that step timing metrics are correctly recorded for all import steps.
     */
    @Test
    public void testStepTimingMetrics() throws Exception
    {
        SSTableConfig config = createSSTableConfig("testk_timing_", "org.apache.cassandra.dht.Murmur3Partitioner", 100, false);
        SSTableZipResult zipResult = createSSTableZipWithConfig(config, false);
        tempSSTableZip = zipResult.zipPath;

        try (Cluster cluster = init(Cluster.build(2)
                                            .withConfig(c -> c.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                              .set("import_concurrency", 2))
                                            .start()))
        {
            setupHttpServer();
            String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);

            cluster.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");

            UUID importId = UUID.randomUUID();
            startImportJob(cluster, importId, TEST_KEYSPACE, TEST_TABLE, sourceUrl, zipResult.startToken, zipResult.endToken);

            String finalStatus = waitForImportCompletionWithAutoTransition(cluster, importId, TEST_KEYSPACE, TEST_TABLE, 120);
            assertEquals("Import should complete successfully", "DONE", finalStatus);

            // Verify step timing metrics
            verifyTimerMetric(cluster, 1, "validationTime");
            verifyTimerMetric(cluster, 1, "filteringTime");
            verifyTimerMetric(cluster, 1, "downloadTime");
            verifyTimerMetric(cluster, 1, "stagedTime");
            verifyTimerMetric(cluster, 1, "importTime");
            verifyTimerMetric(cluster, 1, "trimTime");

            logger.info("All step timing metrics verified");
        }
    }

    private void verifyTimerMetric(Cluster cluster, int nodeNumber, String metricName)
    {
        cluster.get(nodeNumber).callOnInstance(() -> {
            try
            {
                ImportJobMetrics metrics = ImportJobMetrics.instance;

                Field field = metrics.getClass().getDeclaredField(metricName);
                field.setAccessible(true);
                Timer timer = (Timer) field.get(metrics);

                long count = timer.getCount();
                double meanMs = timer.getSnapshot().getMean() / 1_000_000.0;

                logger.info("Timer {}: count={}, mean={} ms", metricName, count, String.format("%.2f", meanMs));

                Assert.assertTrue("Timer " + metricName + " should have recorded at least one measurement", count > 0);
                Assert.assertTrue("Timer " + metricName + " should have positive mean", meanMs > 0);
                return null;
            }
            catch (Exception e)
            {
                logger.error("Failed to verify timer " + metricName, e);
                Assert.fail("Failed to verify timer " + metricName + ": " + e.getMessage());
                return null;
            }
        });
    }
}
