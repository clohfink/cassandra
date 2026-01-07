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

package com.netflix.cassandra.importing.steps;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.netflix.cassandra.importing.ImportJobManager;
import com.netflix.cassandra.importing.ImportStatus;
import com.netflix.cassandra.importing.ImportStep;
import com.netflix.cassandra.metrics.ImportJobMetrics;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Downloads zip files from URLs and extracts their contents to a staging directory with retry logic and rate limiting.
 * <p>
 * <b>Import Flow:</b> VALIDATING → FILTERING → [<b>DOWNLOADING</b>] → STAGED → IMPORTING → TRIMMING → DONE
 * <p>
 * <b>Input:</b> Map of URLs to expected file sizes from SourceSelectionStep<br>
 * <b>Output:</b> Staging directory with extracted SSTable files for StagedStep<br>
 * <b>Execution Type:</b> Asynchronous parallel downloads with unzip operations
 * <p>
 * <b>init():</b> Creates staging directory and initiates parallel downloads with extraction to staging/<br>
 * <b>checkComplete():</b> Monitors download futures completion, tracks progress, and performs disk space checks.
 * Returns StagedStep when all downloads/extractions complete successfully.
 */
public class DownloadUnzipStep implements ImportStep
{
    private static final Logger logger = LoggerFactory.getLogger(DownloadUnzipStep.class);
    public static final ScheduledExecutorPlus retryExecutor = executorFactory().scheduled(false, "UrlImportRetry");
    private static final RateLimiter diskWriteRateLimiter = RateLimiter.create(DatabaseDescriptor.getImportDiskThroughputBytesPerSec());

    /**
     * Updates the disk write rate limiter with the current configuration setting.
     * This can be called when the import_disk_throughput configuration changes.
     */
    public static void updateDiskThroughput()
    {
        diskWriteRateLimiter.setRate(DatabaseDescriptor.getImportDiskThroughputBytesPerSec());
    }

    /**
     * Resizes the unzip thread pool to the specified concurrency level.
     * This can be called when the import_concurrency configuration changes.
     */
    public static void resizeUnzipPool(int newConcurrency)
    {
        int currentMax = unzipPool.getMaximumPoolSize();
        if (newConcurrency > currentMax)
        {
            // Increasing: set maximum first, then core
            unzipPool.setMaximumPoolSize(newConcurrency);
            unzipPool.setCorePoolSize(newConcurrency);
        }
        else if (newConcurrency < currentMax)
        {
            // Decreasing: set core first, then maximum
            unzipPool.setCorePoolSize(newConcurrency);
            unzipPool.setMaximumPoolSize(newConcurrency);
        }
        // If equal, no change needed
    }

    private static final int HTTP_OK = 200;
    private static final int HTTP_REQUEST_TIMEOUT = 408;
    private static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final int HTTP_TIMEOUT_MS = 10000;
    private static final ExecutorPlus unzipPool = executorFactory().pooled("ImportUnzipPool", DatabaseDescriptor.getImportConcurrency());

    private final UUID jobId;
    private final String targetKeyspace;
    private final String targetTable;
    private final Map<String, Long> urlSizes;
    private final AtomicInteger downloadedCount = new AtomicInteger(0);
    private final List<AsyncPromise<Void>> downloadFutures;
    private final AtomicReference<Throwable> firstException;
    private final AtomicLong bytesDownloaded = new AtomicLong(0);
    private volatile boolean downloadsStarted = false;
    private Timer.Context downloadTimer;
    private File stagingDirectory;

    public DownloadUnzipStep(UUID jobId, String targetKeyspace, String targetTable, Map<String, Long> urlSizes)
    {
        this.jobId = jobId;
        this.targetKeyspace = targetKeyspace;
        this.targetTable = targetTable;
        this.urlSizes = urlSizes;
        this.downloadFutures = new ArrayList<>();
        this.firstException = new AtomicReference<>();
    }

    @Override
    public void init()
    {
        logger.info("Starting download and unzip phase for import job {}", jobId);

        // Start download timing
        downloadTimer = ImportJobMetrics.instance.startDownloadTimer();

        TableMetadata metadata = Schema.instance.getTableMetadata(targetKeyspace, targetTable);
        Directories dirs = new Directories(metadata);
        File base = dirs.getCFDirectories().get(0);
        File importsDir = new File(base, "imports");
        importsDir.tryCreateDirectories();
        stagingDirectory = new File(importsDir, jobId.toString());
        stagingDirectory.tryCreateDirectories();
        logger.info("Created temporary directory: {}", stagingDirectory.absolutePath());

        startDownloadsInParallel();
        downloadsStarted = true;
    }

    @Override
    public ImportStep checkComplete() throws Exception
    {
        if (!downloadsStarted)
            return this; // Not ready yet, stay in this step

        // Check if any download failed
        Throwable error = firstException.get();
        if (error != null)
            throw new RuntimeException("One or more downloads failed", error);

        // Periodically check disk space during downloads
        try
        {
            ImportStep.checkImportDiskSpace(0);
        }
        catch (ImportDiskSpaceException e)
        {
            ImportJobMetrics.instance.diskSpaceError();
            ImportJobMetrics.instance.downloadStepError();
            logger.error("Disk space check failed during download phase, cleaning up", e);
            cleanupImportFiles();
            throw new RuntimeException("Disk space check failed during download", e);
        }

        // Check if all downloads are complete
        boolean allComplete = true;
        for (AsyncPromise<Void> future : downloadFutures)
        {
            if (!future.isDone())
            {
                allComplete = false;
                break;
            }
        }

        if (allComplete)
        {
            // Stop download timing and tracking
            if (downloadTimer != null)
            {
                downloadTimer.stop();
                downloadTimer = null;
            }
            ImportJobMetrics.instance.stopDownloadTracking(ImportJobManager.getInstance().getJob(jobId));
            logger.info("All {} zip downloads and extractions completed successfully", urlSizes.size());
            return new StagedStep(jobId, targetKeyspace, targetTable, stagingDirectory);
        }

        // Not all downloads complete, stay in this step
        return this;
    }

    private void startDownloadsInParallel()
    {
        if (urlSizes.isEmpty())
            return;
        cleanupImportFiles();

        try
        {
            long totalDownloadSize = urlSizes.values().stream().mapToLong(Long::longValue).sum();
            ImportStep.checkImportDiskSpace(totalDownloadSize);

            // Track total job size in metrics
            ImportJobMetrics.instance.jobSizeTracked(totalDownloadSize);

            // Start tracking download progress
            ImportJobMetrics.instance.startDownloadTracking(ImportJobManager.getInstance().getJob(jobId), totalDownloadSize);

            for (Map.Entry<String, Long> entry : urlSizes.entrySet())
            {
                String urlString = entry.getKey();

                AsyncPromise<Void> downloadFuture = createDownloadFuture(urlString);
                downloadFutures.add(downloadFuture);
            }

            logger.info("Started {} parallel zip downloads and extractions with total expected size: {} bytes",
                        downloadFutures.size(), totalDownloadSize);
        }
        catch (URISyntaxException | ImportDiskSpaceException e)
        {
            ImportJobMetrics.instance.diskSpaceError();
            ImportJobMetrics.instance.downloadStepError();
            logger.error("Disk space check failed before downloads, cleaning up staging directory", e);
            cleanupImportFiles();
            throw new RuntimeException(e);
        }
    }

    private AsyncPromise<Void> createDownloadFuture(String urlString) throws URISyntaxException
    {
        URI url = new URI(urlString);
        String fileName = url.getPath().substring(url.getPath().lastIndexOf('/') + 1);
        String urlHash = hashUrl(urlString);
        File urlHashDirectory = new File(stagingDirectory, urlHash);
        urlHashDirectory.tryCreateDirectories();
        File downloadFile = new File(urlHashDirectory, fileName);
        Long expectedSize = urlSizes.get(urlString);

        if (shouldSkipDownload(urlString, downloadFile, expectedSize))
        {
            logger.info("Skipping download of {} - file already exists with correct size", urlString);
            downloadedCount.incrementAndGet();

            // Track skipped download metrics (still counts as downloaded)
            ImportJobMetrics.instance.fileDownloaded();
            ImportJobMetrics.instance.bytesDownloaded(expectedSize != null ? expectedSize : downloadFile.length());

            return createCompletedFuture();
        }

        AsyncPromise<Void> downloadPromise = downloadAndUnzipFile(urlString, urlHashDirectory, firstException);
        downloadPromise.addListener(() -> {
            if (downloadPromise.isSuccess())
            {
                downloadedCount.incrementAndGet();

                // Track download metrics
                ImportJobMetrics.instance.fileDownloaded();
                ImportJobMetrics.instance.bytesDownloaded(expectedSize != null ? expectedSize : downloadFile.length());

                logger.info("Downloaded and extracted {} to {}", urlString, urlHashDirectory.absolutePath());
            }
        });
        return downloadPromise;
    }

    private AsyncPromise<Void> createCompletedFuture()
    {
        AsyncPromise<Void> completedFuture = new AsyncPromise<>();
        completedFuture.setSuccess(null);
        return completedFuture;
    }

    private void cleanupImportFiles()
    {
        if (stagingDirectory != null && stagingDirectory.exists())
        {
            try
            {
                File[] files = stagingDirectory.tryList();
                if (files != null)
                {
                    int deletedCount = 0;
                    for (File file : files)
                    {
                        if (file.exists())
                        {
                            boolean deleted = file.tryDelete();
                            if (deleted)
                                deletedCount++;
                            else
                                logger.warn("Failed to delete import file: {}", file.absolutePath());
                        }
                    }
                    logger.info("Cleaned up {} import files from staging directory: {}", deletedCount, stagingDirectory.absolutePath());

                    // Try to remove the staging directory itself
                    if (stagingDirectory.tryDelete())
                        logger.info("Removed staging directory: {}", stagingDirectory.absolutePath());
                    else
                        logger.warn("Failed to remove staging directory: {}", stagingDirectory.absolutePath());
                }
            }
            catch (Exception e)
            {
                logger.error("Error during import files cleanup", e);
            }
        }
    }

    @Override
    public ImportStatus getStatus()
    {
        return ImportStatus.DOWNLOADING;
    }

    private static class ByteProgress
    {
        public final long totalBytes;
        public final long downloadedBytes;

        public ByteProgress(long totalBytes, long downloadedBytes)
        {
            this.totalBytes = totalBytes;
            this.downloadedBytes = downloadedBytes;
        }

        public double getProgressRatio()
        {
            return totalBytes > 0 ? (double) downloadedBytes / totalBytes : 0.0;
        }
    }

    private DownloadUnzipStep.ByteProgress calculateByteProgress()
    {
        long totalBytes = urlSizes.values().stream().mapToLong(Long::longValue).sum();
        return new DownloadUnzipStep.ByteProgress(totalBytes, bytesDownloaded.get());
    }

    private Map<String, String> getProgressStatus()
    {
        Map<String, String> status = new HashMap<>();
        int totalCount = urlSizes.size();
        status.put("file_progress", downloadedCount.get() + " of " + totalCount + " zip files downloaded and extracted");

        DownloadUnzipStep.ByteProgress byteProgress = calculateByteProgress();

        if (byteProgress.totalBytes > 0)
        {
            double percentage = byteProgress.getProgressRatio() * 100.0;
            status.put("bytes_progress", String.format("%d of %d bytes (%.1f%%)",
                                                       byteProgress.downloadedBytes, byteProgress.totalBytes, percentage));
        }
        else
            status.put("bytes_progress", byteProgress.downloadedBytes + " bytes downloaded");

        return status;
    }

    @Override
    public double getProgress()
    {
        return calculateByteProgress().getProgressRatio();
    }

    @Override
    public Map<String, String> toStatusMap()
    {
        Map<String, String> status = baseStatusMap();
        status.put("description", "Downloading and extracting zip files from URLs");
        status.putAll(getProgressStatus());
        return status;
    }

    @Override
    public void cleanup()
    {
        // Cancel all ongoing downloads
        if (downloadFutures != null)
        {
            for (AsyncPromise<Void> future : downloadFutures)
            {
                if (!future.isDone())
                    future.cancel(true); // Use interruption for better cleanup
            }
            downloadFutures.clear();
        }

        // Reset state variables
        downloadsStarted = false;
        downloadedCount.set(0);
        if (firstException != null)
            firstException.set(null);

        logger.info("Cancelled {} download operations for import job {}",
                    downloadFutures != null ? downloadFutures.size() : 0,
                    jobId);
    }

    public static String hashUrl(String urlString)
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(urlString.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash)
            {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1)
                    hexString.append('0');
                hexString.append(hex);
            }
            return hexString.substring(0, 16);
        }
        catch (NoSuchAlgorithmException e)
        {
            logger.warn("SHA-256 not available, using fallback hash for URL: {}", urlString);
            return String.valueOf(Math.abs(urlString.hashCode()));
        }
    }

    private static void handleError(String context, Throwable error, AtomicReference<Throwable> errorHandler)
    {
        errorHandler.compareAndSet(null, error);
        logger.error("Error in {}: {}", context, error.getMessage(), error);
    }



    public AsyncPromise<Void> downloadAndUnzipFile(String urlString, File extractionDirectory, AtomicReference<Throwable> errorHandler)
    {
        return downloadAndUnzipFileWithRetry(urlString, extractionDirectory, errorHandler, 0);
    }

    private AsyncPromise<Void> downloadAndUnzipFileWithRetry(String urlString, File extractionDirectory, AtomicReference<Throwable> errorHandler, int attemptNumber)
    {
        AsyncPromise<Void> promise = new AsyncPromise<>();
        
        unzipPool.submit(() -> {
            try
            {
                URL url = new URL(urlString);
                URLConnection connection = url.openConnection();
                byte[] buffer = new byte[8192];
                
                if (connection instanceof HttpURLConnection)
                {
                    HttpURLConnection httpConnection = (HttpURLConnection) connection;
                    httpConnection.setRequestMethod("GET");
                    httpConnection.setRequestProperty("User-Agent", "Netflix-Cassandra/1.0");
                    httpConnection.setConnectTimeout(HTTP_TIMEOUT_MS);
                    httpConnection.setReadTimeout(HTTP_TIMEOUT_MS);
                    
                    int responseCode = httpConnection.getResponseCode();
                    
                    if (responseCode == HTTP_OK)
                    {
                        try (InputStream inputStream = new BufferedInputStream(httpConnection.getInputStream());
                             ZipInputStream zipInputStream = new ZipInputStream(inputStream))
                        {
                            extractZipEntries(zipInputStream, extractionDirectory, buffer);
                            promise.setSuccess(null);
                        }
                    }
                    else if (isRetryableStatusCode(responseCode))
                    {
                        ImportJobMetrics.instance.httpError(responseCode);
                        handleRetry(urlString, extractionDirectory, errorHandler, attemptNumber, promise,
                                   new IOException("HTTP " + responseCode), responseCode);
                    }
                    else
                    {
                        ImportJobMetrics.instance.httpError(responseCode);
                        ImportJobMetrics.instance.downloadStepError();
                        String errorMessage = "HTTP " + responseCode;
                        String httpResponseMsg = httpConnection.getResponseMessage();
                        if (httpResponseMsg != null && !httpResponseMsg.isEmpty())
                        {
                            errorMessage += " - " + httpResponseMsg;
                        }
                        Exception ex = new IOException(errorMessage);
                        handleError("downloading " + urlString, ex, errorHandler);
                        promise.setFailure(ex);
                    }
                }
                else
                {
                    // For non-HTTP connections (like file:// URLs)
                    try (InputStream inputStream = new BufferedInputStream(connection.getInputStream());
                         ZipInputStream zipInputStream = new ZipInputStream(inputStream))
                    {
                        extractZipEntries(zipInputStream, extractionDirectory, buffer);
                        promise.setSuccess(null);
                    }
                }
            }
            catch (ZipException e)
            {
                ImportJobMetrics.instance.zipExtractionError();
                ImportJobMetrics.instance.unzipStepError();
                ImportJobMetrics.instance.fileCorruptionError();
                handleError("unzipping " + urlString, e, errorHandler);
                promise.setFailure(e);
            }
            catch (SocketException | UnknownHostException e)
            {
                ImportJobMetrics.instance.networkError();
                handleError("downloading " + urlString, e, errorHandler);
                promise.setFailure(e);
            }
            catch (Exception e)
            {
                handleError("downloading " + urlString, e, errorHandler);
                promise.setFailure(e);
            }
        });
        
        return promise;
    }
    
    private void extractZipEntries(ZipInputStream zipInputStream, File extractionDirectory, byte[] buffer) throws IOException
    {
        ZipEntry entry;
        int filesExtracted = 0;

        while ((entry = zipInputStream.getNextEntry()) != null)
        {
            if (!entry.isDirectory())
            {
                File outputFile = new File(extractionDirectory, entry.getName());
                String parentPath = outputFile.absolutePath();
                int lastSlash = parentPath.lastIndexOf('/');
                if (lastSlash > 0)
                {
                    File parentDir = new File(parentPath.substring(0, lastSlash));
                    parentDir.tryCreateDirectories();
                }

                // full path to work around illegal import
                try (java.io.FileOutputStream outputStream = new java.io.FileOutputStream(outputFile.toJavaIOFile()))
                {
                    int bytesRead;
                    while ((bytesRead = zipInputStream.read(buffer)) != -1)
                    {
                        // Apply rate limiting
                        diskWriteRateLimiter.acquire(bytesRead);
                        outputStream.write(buffer, 0, bytesRead);
                        bytesDownloaded.addAndGet(bytesRead);
                    }
                }
                ImportJobMetrics.instance.fileUnzipped();
                filesExtracted++;
            }
            zipInputStream.closeEntry();
        }

        // Verify at least one file was extracted
        if (filesExtracted == 0)
        {
            throw new IOException("No files extracted from zip - file may be corrupted or empty");
        }
    }

    private static boolean isRetryableStatusCode(int statusCode)
    {
        return (statusCode >= 500 && statusCode < 600) || statusCode == HTTP_REQUEST_TIMEOUT || statusCode == HTTP_TOO_MANY_REQUESTS;
    }

    private void handleRetry(String urlString, File extractionDirectory, AtomicReference<Throwable> errorHandler,
                           int attemptNumber, AsyncPromise<Void> promise, @Nullable Throwable prevError, int statusCode)
    {
        int maxAttempts = DatabaseDescriptor.getImportHttpRetryMaxAttempts();

        if (attemptNumber < maxAttempts - 1)
        {
            int nextAttempt = attemptNumber + 1;
            long delayMs = calculateRetryDelay(nextAttempt);

            String errMessage = prevError != null ? prevError.getMessage() : ("HTTP " + statusCode);
            logger.warn("Download attempt {} failed for {}, retrying in {}ms: {}",
                        nextAttempt, urlString, delayMs, errMessage);

            retryExecutor.schedule(() -> {
                AsyncPromise<Void> retryPromise = downloadAndUnzipFileWithRetry(urlString, extractionDirectory, errorHandler, nextAttempt);
                retryPromise.addListener(() -> {
                    if (retryPromise.isSuccess())
                        promise.setSuccess(null);
                    else
                        promise.setFailure(retryPromise.cause());
                });
                ImportJobMetrics.instance.downloadRetry();
            }, delayMs, TimeUnit.MILLISECONDS);
        }
        else
        {
            ImportJobMetrics.instance.downloadStepError();
            Throwable cause = prevError != null ? prevError : new IOException("HTTP " + statusCode);
            handleError("downloading " + urlString + " (after " + maxAttempts + " attempts)", cause, errorHandler);
            promise.setFailure(cause);
        }
    }

    private static long calculateRetryDelay(int attemptNumber)
    {
        int initialDelayMs = DatabaseDescriptor.getImportHttpRetryInitialDelayMs();
        double backoffMultiplier = DatabaseDescriptor.getImportHttpRetryBackoffMultiplier();
        int maxDelayMs = DatabaseDescriptor.getImportHttpRetryMaxDelayMs();
        double jitterRatio = DatabaseDescriptor.getImportHttpRetryJitterPercentage() / 100.0;

        long baseDelayMs = Math.round(initialDelayMs * Math.pow(backoffMultiplier, attemptNumber - 1));
        double jitterRange = baseDelayMs * jitterRatio;
        // Random value between -jitterRange and +jitterRange
        double jitterOffset = (Math.random() * 2 - 1) * jitterRange;
        long delayWithJitter = Math.round(baseDelayMs + jitterOffset);

        return Math.min(Math.max(0, delayWithJitter), maxDelayMs);
    }

    public static boolean shouldSkipDownload(String urlString, File file, Long expectedSize)
    {
        if (!file.exists())
            return false;

        if (expectedSize == null)
        {
            logger.warn("No expected size found for {}, re-downloading to be safe", urlString);
            return false;
        }

        long actualSize = file.length();
        if (actualSize != expectedSize)
        {
            ImportJobMetrics.instance.fileSizeMismatch();
            logger.warn("File {} has incorrect size (expected: {}, actual: {}), re-downloading",
                        file.absolutePath(), expectedSize, actualSize);
            return false;
        }

        return true;
    }
}