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

package com.netflix.cassandra.importing;

import java.util.Map;

public interface ImportJobManagerMBean
{
    /**
     * Get the maximum number of concurrent import operations
     */
    public int getImportConcurrency();

    /**
     * Get the maximum disk usage percentage for import operations
     */
    public int getImportMaxDiskPercentage();

    /**
     * Get the maximum number of HTTP retry attempts for import operations
     */
    public int getImportHttpRetryMaxAttempts();

    /**
     * Set the maximum number of HTTP retry attempts for import operations
     * @param maxAttempts New maximum number of retry attempts
     */
    public void setImportHttpRetryMaxAttempts(int maxAttempts);

    /**
     * Get the backoff multiplier for HTTP retries
     */
    public double getImportHttpRetryBackoffMultiplier();

    /**
     * Set the backoff multiplier for HTTP retries
     * @param backoffMultiplier New backoff multiplier
     */
    public void setImportHttpRetryBackoffMultiplier(double backoffMultiplier);

    /**
     * Get the initial delay in milliseconds for HTTP retries
     */
    public int getImportHttpRetryInitialDelayMs();

    /**
     * Set the initial delay in milliseconds for HTTP retries
     * @param initialDelayMs New initial delay in milliseconds
     */
    public void setImportHttpRetryInitialDelayMs(int initialDelayMs);

    /**
     * Get the maximum delay in milliseconds for HTTP retries
     */
    public int getImportHttpRetryMaxDelayMs();

    /**
     * Set the maximum delay in milliseconds for HTTP retries
     * @param maxDelayMs New maximum delay in milliseconds
     */
    public void setImportHttpRetryMaxDelayMs(int maxDelayMs);

    /**
     * Get the jitter percentage for HTTP retries
     */
    public int getImportHttpRetryJitterPercentage();

    /**
     * Set the jitter percentage for HTTP retries
     * @param jitterPercentage New jitter percentage
     */
    public void setImportHttpRetryJitterPercentage(int jitterPercentage);

    /**
     * Get the disk throughput limit in bytes per second for import operations
     */
    public double getImportDiskThroughputBytesPerSec();

    /**
     * Set the disk throughput limit in bytes per second for import operations
     * @param bytesPerSec New throughput limit in bytes per second
     */
    public void setImportDiskThroughputBytesPerSec(double bytesPerSec);

    /**
     * Get the initial delay in seconds before the first cleanup task runs
     */
    public int getImportCleanupInitialDelaySeconds();

    /**
     * Get the period in seconds between cleanup task runs
     */
    public int getImportCleanupPeriodSeconds();

    /**
     * Get the minimum age in seconds for orphaned directories to be cleaned up
     */
    public int getImportCleanupMinAgeSeconds();

    /**
     * Manually trigger cleanup of orphaned import jobs and directories
     */
    public void cleanupOrphanedJobs();

    /**
     * Get the number of active import jobs
     */
    public int getActiveJobCount();

    /**
     * Set the maximum number of concurrent import operations
     * This will resize the import unzip thread pool accordingly
     * @param concurrency New maximum number of concurrent operations
     */
    public void setImportConcurrency(int concurrency);

    /**
     * Set the maximum disk usage percentage for import operations
     * @param percentage New maximum disk usage percentage
     */
    public void setImportMaxDiskPercentage(int percentage);

    /**
     * Set the initial delay in seconds before the first cleanup task runs
     * This will reschedule the cleanup task
     * @param seconds New initial delay in seconds
     */
    public void setImportCleanupInitialDelaySeconds(int seconds);

    /**
     * Set the period in seconds between cleanup task runs
     * This will reschedule the cleanup task
     * @param seconds New period in seconds
     */
    public void setImportCleanupPeriodSeconds(int seconds);

    /**
     * Set the minimum age in seconds for orphaned directories to be cleaned up
     * @param seconds New minimum age in seconds
     */
    public void setImportCleanupMinAgeSeconds(int seconds);

    /**
     * Snapshot of all active import jobs on this node. The returned map is
     * insertion-ordered by job id and contains one entry per job:
     *
     *   key   = {jobId}
     *   value = "{keyspace}.{table} | {status} | step={step} | progress={pct}"
     *
     * Designed for human-friendly display from nodetool.
     */
    public Map<String, String> getActiveJobs();

    /**
     * Detailed status for a single active job (the same map exposed via the
     * netflix_views.local_import virtual table). Returns an empty map if no
     * job with the given id is tracked on this node.
     *
     * @param jobId UUID string of the import job
     */
    public Map<String, String> getJobStatus(String jobId);

    /**
     * Cancel a single in-flight import job. The job is moved to CANCELLED
     * state, any in-flight downloads are interrupted, and the staging
     * directory is cleaned up.
     *
     * @param jobId UUID string of the import job
     * @return true if a job with that id was found and cancellation was
     *         attempted; false if no such job exists on this node.
     */
    public boolean cancelJob(String jobId);

    /**
     * Returns the full set of hot-tunable import configuration values
     * (insertion-ordered for human-friendly display).
     */
    public Map<String, String> getConfiguration();

    /**
     * Set a single hot-tunable configuration value by name. The accepted
     * keys mirror those returned by {@link #getConfiguration()}.
     *
     * @param name  configuration key (e.g. "import_concurrency",
     *              "import_http_retry_max_attempts")
     * @param value string representation of the new value
     * @throws IllegalArgumentException if the key is unknown or the value
     *         cannot be parsed for that key
     */
    public void setConfiguration(String name, String value);
}