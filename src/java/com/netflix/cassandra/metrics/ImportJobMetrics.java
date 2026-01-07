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

package com.netflix.cassandra.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.netflix.cassandra.importing.ImportJob;
import com.netflix.cassandra.importing.ImportStatus;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.utils.Clock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportJobMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(ImportJobMetrics.class);
    private static final MetricNameFactory factory = new DefaultNameFactory("ImportJob");
    
    // Singleton instance to ensure MBean registration happens only once
    public static final ImportJobMetrics instance = new ImportJobMetrics();
    
    // Job lifecycle counters
    public final Counter jobsStarted = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsStarted"));
    public final Counter jobsCompleted = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsCompleted"));
    public final Counter jobsFailed = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsFailed"));
    public final Counter jobsCancelled = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsCancelled"));
    
    // Per-status counters
    public final Counter jobsDownloading = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsDownloading"));
    public final Counter jobsUnzipping = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsUnzipping"));
    public final Counter jobsStaged = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsStaged"));
    public final Counter jobsImporting = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsImporting"));
    public final Counter jobsTrimming = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("JobsTrimming"));
    
    // Error counters by category
    public final Counter networkErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("NetworkErrors"));
    public final Counter httpErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("HttpErrors"));
    public final Counter diskSpaceErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("DiskSpaceErrors"));
    public final Counter fileCorruptionErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("FileCorruptionErrors"));
    public final Counter timeoutErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("TimeoutErrors"));
    public final Counter validationErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ValidationErrors"));
    public final Counter sstableImportErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("SstableImportErrors"));
    public final Counter configurationErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ConfigurationErrors"));
    public final Counter cleanupErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("CleanupErrors"));
    
    // Error counters by step
    public final Counter downloadStepErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("DownloadStepErrors"));
    public final Counter unzipStepErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("UnzipStepErrors"));
    public final Counter importStepErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ImportStepErrors"));
    public final Counter trimStepErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("TrimStepErrors"));
    
    // Specific error counters
    public final Counter downloadRetries = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("DownloadRetries"));
    public final Counter fileSizeMismatches = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("FileSizeMismatches"));
    public final Counter httpStatusErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("HttpStatusErrors"));
    public final Counter zipExtractionErrors = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ZipExtractionErrors"));
    public final Counter consistencyLevelFallbacks = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("ConsistencyLevelFallbacks"));
    
    // Timing metrics
    public final Timer jobDuration = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("JobDuration"));
    public final Timer validationTime = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("ValidationTime"));
    public final Timer filteringTime = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("FilteringTime"));
    public final Timer downloadTime = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("DownloadTime"));
    public final Timer unzipTime = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("UnzipTime"));
    public final Timer stagedTime = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("StagedTime"));
    public final Timer importTime = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("ImportTime"));
    public final Timer trimTime = CassandraMetricsRegistry.Metrics.timer(factory.createMetricName("TrimTime"));
    
    // Data size metrics
    public final Counter bytesDownloaded = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("BytesDownloaded"));
    public final Counter filesDownloaded = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("FilesDownloaded"));
    public final Counter filesUnzipped = CassandraMetricsRegistry.Metrics.counter(factory.createMetricName("FilesUnzipped"));
    
    // Size distribution metrics
    public final Histogram downloadSizeDistribution = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("DownloadSizeDistribution"), false);
    public final Histogram jobSizeDistribution = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("JobSizeDistribution"), false);
    
    // Active job tracking
    private final ConcurrentMap<String, ImportJob> activeJobs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> jobStartTimes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> jobDownloadSizes = new ConcurrentHashMap<>();

    // Last recorded step durations (in milliseconds) for displaying most recent values
    private final ConcurrentMap<ImportStatus, Long> lastStepDurations = new ConcurrentHashMap<>();
    
    // Gauges for current state
    public final Gauge<Integer> activeJobCount = CassandraMetricsRegistry.Metrics.register(
        factory.createMetricName("ActiveJobs"),
        new Gauge<Integer>()
        {
            @Override
            public Integer getValue()
            {
                return activeJobs.size();
            }
        }
    );
    
    public final Gauge<Long> longestRunningJobDuration = CassandraMetricsRegistry.Metrics.register(
        factory.createMetricName("LongestRunningJobDuration"),
        new Gauge<Long>()
        {
            @Override
            public Long getValue()
            {
                long now = Clock.Global.currentTimeMillis();
                long maxDuration = 0;
                for (Long startTime : jobStartTimes.values())
                {
                    long duration = now - startTime;
                    if (duration > maxDuration)
                        maxDuration = duration;
                }
                return maxDuration;
            }
        }
    );
    
    public final Gauge<Long> bytesDownloading = CassandraMetricsRegistry.Metrics.register(
        factory.createMetricName("BytesDownloading"),
        new Gauge<Long>()
        {
            @Override
            public Long getValue()
            {
                return jobDownloadSizes.values().stream().mapToLong(Long::longValue).sum();
            }
        }
    );
    
    // Note: Queue size gauge removed as ScheduledExecutorPlus doesn't expose queue size
    
    private ImportJobMetrics()
    {
        // Private constructor for singleton
    }
    
    // Job lifecycle tracking methods
    public void jobStarted(ImportJob job)
    {
        jobsStarted.inc();
        String jobId = job.toString();
        activeJobs.put(jobId, job);
        jobStartTimes.put(jobId, Clock.Global.currentTimeMillis());
    }
    
    public void jobCompleted(ImportJob job)
    {
        jobsCompleted.inc();
        String jobId = job.toString();
        activeJobs.remove(jobId);
        jobDownloadSizes.remove(jobId);
        Long startTime = jobStartTimes.remove(jobId);
        if (startTime != null)
        {
            jobDuration.update(Clock.Global.currentTimeMillis() - startTime, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }
    
    public void jobFailed(ImportJob job)
    {
        jobsFailed.inc();
        String jobId = job.toString();
        activeJobs.remove(jobId);
        jobDownloadSizes.remove(jobId);
        Long startTime = jobStartTimes.remove(jobId);
        if (startTime != null)
        {
            jobDuration.update(Clock.Global.currentTimeMillis() - startTime, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }
    
    public void jobCancelled(ImportJob job)
    {
        jobsCancelled.inc();
        String jobId = job.toString();
        activeJobs.remove(jobId);
        jobDownloadSizes.remove(jobId);
        Long startTime = jobStartTimes.remove(jobId);
        if (startTime != null)
        {
            jobDuration.update(Clock.Global.currentTimeMillis() - startTime, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
    }
    
    // Status transition tracking
    public void statusChanged(ImportStatus status)
    {
        switch (status)
        {
            case DOWNLOADING:
                jobsDownloading.inc();
                break;
            case UNZIPPING:
                jobsUnzipping.inc();
                break;
            case STAGED:
                jobsStaged.inc();
                break;
            case IMPORTING:
                jobsImporting.inc();
                break;
            case TRIMMING:
                jobsTrimming.inc();
                break;
        }
    }
    
    // Data metrics tracking
    public void bytesDownloaded(long bytes)
    {
        bytesDownloaded.inc(bytes);
        downloadSizeDistribution.update(bytes);
    }
    
    public void fileDownloaded()
    {
        filesDownloaded.inc();
    }
    
    public void fileUnzipped()
    {
        filesUnzipped.inc();
    }
    
    public void jobSizeTracked(long totalSize)
    {
        jobSizeDistribution.update(totalSize);
    }
    
    public void startDownloadTracking(ImportJob job, long expectedBytes)
    {
        String jobId = job.toString();
        jobDownloadSizes.put(jobId, expectedBytes);
    }
    
    public void stopDownloadTracking(ImportJob job)
    {
        String jobId = job.toString();
        jobDownloadSizes.remove(jobId);
    }
    
    // Timer context methods for step timing
    public Timer.Context startDownloadTimer()
    {
        return downloadTime.time();
    }

    public Timer.Context startImportTimer()
    {
        return importTime.time();
    }

    public Timer.Context startTrimTimer()
    {
        return trimTime.time();
    }

    // Record step duration based on status
    public void recordStepDuration(ImportStatus status, long durationMs)
    {
        // Record using manual timer update - must use nanoseconds to match Timer.Context behavior
        // Timer.Context uses Clock.getTick() which returns nanoseconds
        // Ensure minimum of 1ms to avoid zero values
        long durationNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1, durationMs));

        // Always track the most recent duration for this step type
        lastStepDurations.put(status, durationMs);

        switch (status)
        {
            case VALIDATING:
                logger.debug("Recording step duration: status={}, durationMs={}, durationNanos={}", status, durationMs, durationNanos);
                validationTime.update(durationNanos, TimeUnit.NANOSECONDS);
                break;
            case FILTERING:
                logger.debug("Recording step duration: status={}, durationMs={}, durationNanos={}", status, durationMs, durationNanos);
                filteringTime.update(durationNanos, TimeUnit.NANOSECONDS);
                break;
            case DOWNLOADING:
                logger.debug("Recording step duration: status={}, durationMs={} (using Timer.Context)", status, durationMs);
                // Note: download step already uses timer context, but we still track last duration
                break;
            case UNZIPPING:
                logger.debug("Recording step duration: status={}, durationMs={}, durationNanos={}", status, durationMs, durationNanos);
                unzipTime.update(durationNanos, TimeUnit.NANOSECONDS);
                break;
            case STAGED:
                logger.debug("Recording step duration: status={}, durationMs={}, durationNanos={}", status, durationMs, durationNanos);
                stagedTime.update(durationNanos, TimeUnit.NANOSECONDS);
                break;
            case IMPORTING:
                logger.debug("Recording step duration: status={}, durationMs={} (using Timer.Context)", status, durationMs);
                // Note: import step already uses timer context, but we still track last duration
                break;
            case TRIMMING:
                logger.debug("Recording step duration: status={}, durationMs={} (using Timer.Context)", status, durationMs);
                // Note: trim step already uses timer context, but we still track last duration
                break;
        }
    }

    // Get the most recently recorded duration for a step (in milliseconds)
    public Long getLastStepDuration(ImportStatus status)
    {
        return lastStepDurations.get(status);
    }
    
    // Error tracking methods
    public void networkError()
    {
        networkErrors.inc();
    }
    
    public void httpError(int statusCode)
    {
        httpErrors.inc();
        if (statusCode != 200)
        {
            httpStatusErrors.inc();
        }
    }
    
    public void diskSpaceError()
    {
        diskSpaceErrors.inc();
    }
    
    public void fileCorruptionError()
    {
        fileCorruptionErrors.inc();
    }
    
    public void timeoutError()
    {
        timeoutErrors.inc();
    }
    
    public void validationError()
    {
        validationErrors.inc();
    }
    
    public void sstableImportError(int failedCount)
    {
        sstableImportErrors.inc(failedCount);
    }
    
    public void configurationError()
    {
        configurationErrors.inc();
    }

    public void cleanupError()
    {
        cleanupErrors.inc();
    }
    
    public void downloadStepError()
    {
        downloadStepErrors.inc();
    }
    
    public void unzipStepError()
    {
        unzipStepErrors.inc();
    }
    
    public void importStepError()
    {
        importStepErrors.inc();
    }
    
    public void trimStepError()
    {
        trimStepErrors.inc();
    }
    
    public void downloadRetry()
    {
        downloadRetries.inc();
    }
    
    public void fileSizeMismatch()
    {
        fileSizeMismatches.inc();
    }
    
    public void zipExtractionError()
    {
        zipExtractionErrors.inc();
    }
    
    public void consistencyLevelFallback()
    {
        consistencyLevelFallbacks.inc();
    }
}