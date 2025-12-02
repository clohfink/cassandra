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

package com.netflix.cassandra.backups;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.utils.concurrent.AsyncPromise;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AwsAsyncS3FakeBackupTest
{
    private AwsAsyncS3FakeBackup s3Fake;
    private Path tempDir;
    private String testBucket = "test-bucket";
    private String testKey = "test-key";
    private byte[] testData = "Hello, S3 World!".getBytes();

    @Before
    public void setUp() throws IOException
    {
        tempDir = Files.createTempDirectory("s3-fake-test");
        
        Map<String, String> envVars = new HashMap<>();
        s3Fake = new AwsAsyncS3FakeBackup(envVars, Region.US_EAST_1);
        s3Fake.setFakeS3RootDir(tempDir.toString());
        
        // Create test data structure
        setupTestData();
    }

    @After
    public void tearDown() throws IOException
    {
        // Clean up temp directory
        Files.walk(tempDir)
            .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
            .forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    // Ignore cleanup failures in tests
                }
            });
    }

    private void setupTestData() throws IOException
    {
        Path bucketPath = tempDir.resolve(testBucket);
        Files.createDirectories(bucketPath);
        
        Path filePath = bucketPath.resolve(testKey);
        Files.write(filePath, testData);
        
        // Create additional test files with directory structure
        Files.createDirectories(bucketPath.resolve("prefix"));
        Files.createDirectories(bucketPath.resolve("other"));
        Files.write(bucketPath.resolve("prefix/file1.txt"), "file1 content".getBytes());
        Files.write(bucketPath.resolve("prefix/file2.txt"), "file2 content".getBytes());
        Files.write(bucketPath.resolve("other/file3.txt"), "file3 content".getBytes());
    }

    @Test
    public void testConstructor()
    {
        Map<String, String> envVars = new HashMap<>();
        envVars.put("TEST_VAR", "value");
        
        AwsAsyncS3FakeBackup s3 = new AwsAsyncS3FakeBackup(envVars, Region.US_WEST_2);
        assertNotNull(s3);
    }

    @Test
    public void testGetObjectAsFileSuccess() throws Exception
    {
        Path targetPath = tempDir.resolve("downloaded-file");
        
        AsyncPromise<Void> result = s3Fake.getObjectAsFile(testBucket, testKey, targetPath);
        result.get();
        
        assertTrue(Files.exists(targetPath));
        assertArrayEquals(testData, Files.readAllBytes(targetPath));
    }

    @Test
    public void testGetObjectAsFileNotFound() throws Exception
    {
        Path targetPath = tempDir.resolve("downloaded-file");
        
        AsyncPromise<Void> result = s3Fake.getObjectAsFile(testBucket, "nonexistent", targetPath);
        
        try {
            result.get();
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().contains("Source file does not exist"));
        }
    }

    @Test
    public void testGetObjectRangeIntoBufferSuccess() throws Exception
    {
        long from = 7;
        long to = 9; // Note: this method uses inclusive 'to', unlike getObjectRange
        ByteBuffer buffer = ByteBuffer.allocate(10);
        
        AsyncPromise<Void> result = s3Fake.getObjectRangeIntoBuffer(testBucket, testKey, from, to, buffer);
        result.get();
        
        buffer.flip();
        byte[] resultData = new byte[buffer.remaining()];
        buffer.get(resultData);
        assertEquals("S3 ", new String(resultData));
    }

    @Test
    public void testGetObjectRangeIntoBufferTooSmall()
    {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        
        try {
            s3Fake.getObjectRangeIntoBuffer(testBucket, testKey, 0, 10, buffer);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Buffer too small"));
        }
    }

    @Test
    public void testGetObjectRangeIntoBufferFileNotFound() throws Exception
    {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        
        AsyncPromise<Void> result = s3Fake.getObjectRangeIntoBuffer(testBucket, "nonexistent", 0, 5, buffer);
        
        try {
            result.get();
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().contains("File does not exist"));
        }
    }

    @Test
    public void testGetObjectKeysSuccess() throws Exception
    {
        AsyncPromise<List<String>> result = s3Fake.getObjectKeys(testBucket, "prefix");
        List<String> keys = result.get();
        
        assertNotNull(keys);
        assertTrue(keys.size() >= 2);
        assertTrue(keys.stream().anyMatch(key -> key.contains("file1.txt")));
        assertTrue(keys.stream().anyMatch(key -> key.contains("file2.txt")));
    }

    @Test
    public void testGetObjectKeysEmptyPrefix() throws Exception
    {
        AsyncPromise<List<String>> result = s3Fake.getObjectKeys(testBucket, "");
        List<String> keys = result.get();
        
        assertNotNull(keys);
        assertTrue(keys.size() >= 4); // testKey + 3 additional files
    }

    @Test
    public void testGetObjectKeysBucketNotFound() throws Exception
    {
        AsyncPromise<List<String>> result = s3Fake.getObjectKeys("nonexistent-bucket", "");
        List<String> keys = result.get();
        
        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testGetObjectSizeSuccess() throws Exception
    {
        AsyncPromise<Long> result = s3Fake.getObjectSize(testBucket, testKey);
        Long size = result.get();
        
        assertEquals(Long.valueOf(testData.length), size);
    }

    @Test
    public void testGetObjectSizeFileNotFound() throws Exception
    {
        AsyncPromise<Long> result = s3Fake.getObjectSize(testBucket, "nonexistent");
        
        try {
            result.get();
            fail("Expected exception");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertTrue(e.getCause().getMessage().contains("File does not exist"));
        }
    }

}