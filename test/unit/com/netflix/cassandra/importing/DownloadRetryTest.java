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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.netflix.cassandra.importing.steps.DownloadUnzipStep;
import com.netflix.cassandra.metrics.ImportJobMetrics;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static org.junit.Assert.*;

/**
 * Unit tests for DownloadUnzipStep retry logic and error handling.
 *
 * This test class covers:
 * - HTTP retry logic with intermittent failures
 * - Retry exhaustion after max attempts
 * - Retryable vs non-retryable HTTP status codes
 * - Metrics verification for retries and failures
 */
public class DownloadRetryTest
{
    private HttpServer httpServer;
    private final int serverPort = 8991;
    private Path tempZipFile;
    private Path stagingDir;

    @Before
    public void setUp() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        createTestZipFile();
        stagingDir = Files.createTempDirectory("import-test-staging");
    }

    @After
    public void tearDown() throws Exception
    {
        if (httpServer != null)
        {
            httpServer.stop(0);
        }
        if (tempZipFile != null && Files.exists(tempZipFile))
        {
            Files.delete(tempZipFile);
        }
        if (stagingDir != null && Files.exists(stagingDir))
        {
            // Clean up staging directory
            Files.walk(stagingDir)
                 .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                 .forEach(path -> {
                     try { Files.deleteIfExists(path); } catch (IOException e) { /* ignore */ }
                 });
        }
    }

    /**
     * Test that DownloadUnzipStep retries on retryable HTTP errors (5xx).
     * Server fails twice with 500, then succeeds on third attempt.
     */
    @Test
    public void testRetryOnIntermittentFailure() throws Exception
    {
        AtomicInteger attemptCount = new AtomicInteger(0);
        final int failuresBeforeSuccess = 2;

        httpServer = HttpServer.create(new InetSocketAddress(serverPort), 0);
        httpServer.createContext("/sstable.zip", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                int attempt = attemptCount.incrementAndGet();

                if (attempt <= failuresBeforeSuccess)
                {
                    // Fail with 500 Internal Server Error
                    exchange.sendResponseHeaders(500, -1);
                    exchange.close();
                }
                else
                {
                    // Succeed on third attempt
                    byte[] content = Files.readAllBytes(tempZipFile);
                    exchange.sendResponseHeaders(200, content.length);
                    exchange.getResponseBody().write(content);
                    exchange.close();
                }
            }
        });
        httpServer.start();

        // Create DownloadUnzipStep
        String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);
        Map<String, Long> urlSizes = new HashMap<>();
        urlSizes.put(sourceUrl, Files.size(tempZipFile));

        DownloadUnzipStep step = new DownloadUnzipStep(UUID.randomUUID(), "testks", "testtable", urlSizes);

        // Capture initial retry metric
        long initialRetries = ImportJobMetrics.instance.downloadRetries.getCount();

        // Execute download
        AtomicReference<Throwable> errorHandler = new AtomicReference<>();
        File extractionDir = new File(stagingDir.toFile());
        AsyncPromise<Void> promise = step.downloadAndUnzipFile(sourceUrl, extractionDir, errorHandler);

        // Wait for completion with timeout
        boolean completed = promise.await(30, TimeUnit.SECONDS);
        assertTrue("Download should complete successfully after retries", completed);
        assertTrue("Promise should succeed", promise.isSuccess());
        assertNull("Should not have error", errorHandler.get());

        // Verify attempts made
        assertEquals("Should make 3 attempts total (2 failures + 1 success)",
                    failuresBeforeSuccess + 1, attemptCount.get());

        // Verify retry metric incremented
        long finalRetries = ImportJobMetrics.instance.downloadRetries.getCount();
        assertTrue("Retry metric should increase", finalRetries > initialRetries);

        // Verify file was extracted
        assertTrue("Staging directory should contain extracted files", extractionDir.tryList().length > 0);
    }

    /**
     * Test that DownloadUnzipStep fails after exhausting all retry attempts.
     */
    @Test
    public void testRetryExhaustion() throws Exception
    {
        AtomicInteger attemptCount = new AtomicInteger(0);

        httpServer = HttpServer.create(new InetSocketAddress(serverPort), 0);
        httpServer.createContext("/sstable.zip", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                attemptCount.incrementAndGet();
                // Always fail with 503 Service Unavailable (retryable)
                exchange.sendResponseHeaders(503, -1);
                exchange.close();
            }
        });
        httpServer.start();

        String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);
        Map<String, Long> urlSizes = new HashMap<>();
        urlSizes.put(sourceUrl, Files.size(tempZipFile));

        DownloadUnzipStep step = new DownloadUnzipStep(UUID.randomUUID(), "testks", "testtable", urlSizes);

        // Capture initial failure metric
        long initialHttpErrors = ImportJobMetrics.instance.httpErrors.getCount();

        // Execute download
        AtomicReference<Throwable> errorHandler = new AtomicReference<>();
        File extractionDir = new File(stagingDir.toFile());
        AsyncPromise<Void> promise = step.downloadAndUnzipFile(sourceUrl, extractionDir, errorHandler);

        // Wait for completion/failure
        boolean completed = promise.await(30, TimeUnit.SECONDS);
        assertTrue("Download should complete (with failure) within timeout", completed);
        assertFalse("Promise should fail after retry exhaustion", promise.isSuccess());
        assertNotNull("Should have error in promise", promise.cause());

        // Get max retry attempts from config
        int maxAttempts = DatabaseDescriptor.getImportHttpRetryMaxAttempts();

        // Verify all retry attempts were made
        assertEquals("Should exhaust all retry attempts", maxAttempts, attemptCount.get());

        // Verify error metrics incremented
        long finalHttpErrors = ImportJobMetrics.instance.httpErrors.getCount();
        assertTrue("HTTP error metric should increase", finalHttpErrors > initialHttpErrors);

        // Verify no files were extracted
        assertEquals("Staging directory should be empty on failure", 0, extractionDir.tryList().length);
    }

    /**
     * Test that non-retryable HTTP errors (4xx) fail immediately without retry.
     */
    @Test
    public void testNonRetryableStatusCode() throws Exception
    {
        AtomicInteger attemptCount = new AtomicInteger(0);

        httpServer = HttpServer.create(new InetSocketAddress(serverPort), 0);
        httpServer.createContext("/sstable.zip", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                attemptCount.incrementAndGet();
                // Return 404 Not Found (non-retryable)
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
            }
        });
        httpServer.start();

        String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);
        Map<String, Long> urlSizes = new HashMap<>();
        urlSizes.put(sourceUrl, Files.size(tempZipFile));

        DownloadUnzipStep step = new DownloadUnzipStep(UUID.randomUUID(), "testks", "testtable", urlSizes);

        // Execute download
        AtomicReference<Throwable> errorHandler = new AtomicReference<>();
        File extractionDir = new File(stagingDir.toFile());
        AsyncPromise<Void> promise = step.downloadAndUnzipFile(sourceUrl, extractionDir, errorHandler);

        // Wait for completion/failure
        boolean completed = promise.await(10, TimeUnit.SECONDS);
        assertTrue("Download should fail quickly for non-retryable error", completed);
        assertFalse("Promise should fail", promise.isSuccess());

        // Verify only one attempt made (no retries)
        assertEquals("Should make only 1 attempt for non-retryable error", 1, attemptCount.get());

        // Verify error was captured
        assertNotNull("Should have error in promise", promise.cause());
        assertTrue("Error message should mention HTTP 404",
                  promise.cause().getMessage().contains("404"));
    }

    /**
     * Test that retryable status codes (5xx, 408, 429) trigger retry logic.
     */
    @Test
    public void testRetryableStatusCodes() throws Exception
    {
        // Test various retryable status codes
        int[] retryableStatusCodes = {500, 502, 503, 504, 408, 429};

        for (int statusCode : retryableStatusCodes)
        {
            AtomicInteger attemptCount = new AtomicInteger(0);

            httpServer = HttpServer.create(new InetSocketAddress(serverPort), 0);
            httpServer.createContext("/sstable.zip", new HttpHandler()
            {
                @Override
                public void handle(HttpExchange exchange) throws IOException
                {
                    int attempt = attemptCount.incrementAndGet();

                    if (attempt == 1)
                    {
                        // Fail on first attempt with the test status code
                        exchange.sendResponseHeaders(statusCode, -1);
                        exchange.close();
                    }
                    else
                    {
                        // Succeed on retry
                        byte[] content = Files.readAllBytes(tempZipFile);
                        exchange.sendResponseHeaders(200, content.length);
                        exchange.getResponseBody().write(content);
                        exchange.close();
                    }
                }
            });
            httpServer.start();

            String sourceUrl = String.format("http://127.0.0.1:%d/sstable.zip", serverPort);
            Map<String, Long> urlSizes = new HashMap<>();
            urlSizes.put(sourceUrl, Files.size(tempZipFile));

            DownloadUnzipStep step = new DownloadUnzipStep(UUID.randomUUID(), "testks", "testtable", urlSizes);

            // Execute download
            AtomicReference<Throwable> errorHandler = new AtomicReference<>();
            File extractionDir = new File(stagingDir.toFile());
            AsyncPromise<Void> promise = step.downloadAndUnzipFile(sourceUrl, extractionDir, errorHandler);

            // Wait for completion
            boolean completed = promise.await(20, TimeUnit.SECONDS);
            assertTrue(String.format("HTTP %d should be retryable and eventually succeed", statusCode),
                      completed && promise.isSuccess());

            // Verify retry occurred
            assertTrue(String.format("HTTP %d should trigger retry", statusCode),
                      attemptCount.get() > 1);

            // Cleanup for next iteration
            httpServer.stop(0);
            httpServer = null;

            // Clean staging dir
            for (File file : extractionDir.tryList())
            {
                file.delete();
            }
        }
    }

    /**
     * Test that corrupted zip file is handled gracefully.
     */
    @Test
    public void testCorruptedZipFile() throws Exception
    {
        // Create corrupted zip file (partial header)
        Path corruptedZip = Files.createTempFile("corrupted", ".zip");
        Files.write(corruptedZip, new byte[]{0x50, 0x4B, 0x03, 0x04, 0x00, 0x00});

        httpServer = HttpServer.create(new InetSocketAddress(serverPort), 0);
        httpServer.createContext("/corrupted.zip", new HttpHandler()
        {
            @Override
            public void handle(HttpExchange exchange) throws IOException
            {
                byte[] content = Files.readAllBytes(corruptedZip);
                exchange.sendResponseHeaders(200, content.length);
                exchange.getResponseBody().write(content);
                exchange.close();
            }
        });
        httpServer.start();

        String sourceUrl = String.format("http://127.0.0.1:%d/corrupted.zip", serverPort);
        Map<String, Long> urlSizes = new HashMap<>();
        urlSizes.put(sourceUrl, Files.size(corruptedZip));

        DownloadUnzipStep step = new DownloadUnzipStep(UUID.randomUUID(), "testks", "testtable", urlSizes);

        // Execute download
        AtomicReference<Throwable> errorHandler = new AtomicReference<>();
        File extractionDir = new File(stagingDir.toFile());
        AsyncPromise<Void> promise = step.downloadAndUnzipFile(sourceUrl, extractionDir, errorHandler);

        // Wait for failure
        boolean completed = promise.await(10, TimeUnit.SECONDS);
        assertTrue("Download should complete (with failure)", completed);
        assertFalse("Promise should fail due to corrupted zip", promise.isSuccess());

        // Verify error is related to zip corruption
        assertNotNull("Should have error", promise.cause());

        // Cleanup
        Files.delete(corruptedZip);
    }

    // Helper methods

    private void createTestZipFile() throws Exception
    {
        tempZipFile = Files.createTempFile("test", ".zip");

        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(tempZipFile)))
        {
            // Add a simple text file to the zip
            ZipEntry entry = new ZipEntry("test-data.db");
            zos.putNextEntry(entry);
            zos.write("Test SSTable content for download retry tests".getBytes());
            zos.closeEntry();
        }
    }
}
