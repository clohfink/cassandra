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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackupMemtableContextTest
{
    private static final String BUCKET = "test-bucket";
    private static final String PREFIX = "test/prefix";
    private static final String TOKEN = "3";

    private AwsAsyncS3FakeBackup fakeS3;
    private Map<String, String> envVars;
    private Path tempDir;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup() throws IOException
    {
        tempDir = Files.createTempDirectory("context-test");
        envVars = new HashMap<>();
        envVars.put("NETFLIX_REGION", "us-east-1");
        envVars.put("NETFLIX_APP", "testapp");
        envVars.put("NETFLIX_ENVIRONMENT", "test");

        fakeS3 = new AwsAsyncS3FakeBackup(envVars, Region.US_EAST_1);
        fakeS3.setFakeS3RootDir(tempDir.toString());
        ObjectStoreAccess.set(Region.US_EAST_1, fakeS3);
    }

    @After
    public void tearDown() throws IOException
    {
        Files.walk(tempDir)
             .sorted((a, b) -> b.compareTo(a))
             .forEach(path -> {
                 try { Files.delete(path); }
                 catch (IOException ignored) {}
             });
    }

    /**
     * Creates a fake meta file in the fake S3 directory structure.
     * Key format: PREFIX/TOKEN/META_V2/timestamp/ks/tbl/meta_v2.json
     * extractTimestampFromKey expects timestamp at segments[length - 4].
     */
    private void createFakeMetaFile(long timestamp, String content) throws IOException
    {
        Path dir = tempDir.resolve(BUCKET)
                          .resolve(PREFIX)
                          .resolve(TOKEN)
                          .resolve("META_V2")
                          .resolve(String.valueOf(timestamp))
                          .resolve("ks")
                          .resolve("tbl");
        Files.createDirectories(dir);
        Files.write(dir.resolve("meta_v2.json"), content.getBytes());
    }

    private BackupMemtableContext createContext(long timestamp)
    {
        String config = String.format(
            "backupmemtable:prefix=%s,bucket=%s,keyspace=testks,token=%s,table=testtable,timestamp=%d",
            PREFIX, BUCKET, TOKEN, timestamp);
        BackupMemtableParams params = new BackupMemtableParams(config, envVars);
        return new BackupMemtableContext(params, null);
    }

    private File newTargetFile(String name) throws IOException
    {
        return new File(tempDir.resolve(name).toString());
    }

    @Test
    public void testDownloadClosestMeta_SelectsClosestTimestamp() throws Exception
    {
        createFakeMetaFile(800, "older");
        createFakeMetaFile(900, "closest");

        BackupMemtableContext ctx = createContext(1000);
        File metaFile = newTargetFile("meta.json");
        ctx.downloadClosestMeta(metaFile);

        String content = new String(Files.readAllBytes(metaFile.toPath()));
        assertEquals("closest", content);
    }

    @Test
    public void testDownloadClosestMeta_ExactTimestampMatch() throws Exception
    {
        createFakeMetaFile(500, "older");
        createFakeMetaFile(1000, "exact");

        BackupMemtableContext ctx = createContext(1000);
        File metaFile = newTargetFile("meta.json");
        ctx.downloadClosestMeta(metaFile);

        String content = new String(Files.readAllBytes(metaFile.toPath()));
        assertEquals("exact", content);
    }

    @Test
    public void testDownloadClosestMeta_IgnoresFutureTimestamps() throws Exception
    {
        createFakeMetaFile(500, "past");
        createFakeMetaFile(2000, "future");

        BackupMemtableContext ctx = createContext(1000);
        File metaFile = newTargetFile("meta.json");
        ctx.downloadClosestMeta(metaFile);

        String content = new String(Files.readAllBytes(metaFile.toPath()));
        assertEquals("past", content);
    }

    @Test(expected = RuntimeException.class)
    public void testDownloadClosestMeta_ThrowsWhenAllTimestampsInFuture() throws Exception
    {
        createFakeMetaFile(2000, "future");
        createFakeMetaFile(3000, "more_future");

        BackupMemtableContext ctx = createContext(1000);
        ctx.downloadClosestMeta(newTargetFile("meta.json"));
    }

    @Test(expected = RuntimeException.class)
    public void testDownloadClosestMeta_ThrowsWhenNoMetaFiles() throws Exception
    {
        // Create the prefix directory but with no meta files
        Path dir = tempDir.resolve(BUCKET).resolve(PREFIX).resolve(TOKEN).resolve("META_V2");
        Files.createDirectories(dir);

        BackupMemtableContext ctx = createContext(1000);
        ctx.downloadClosestMeta(newTargetFile("meta.json"));
    }

    @Test
    public void testDownloadClosestMeta_RetriesOnFailureThenSucceeds() throws Exception
    {
        createFakeMetaFile(900, "retried");

        // Inject one failure; second attempt will fall through to normal filesystem read
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Failure<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_AS_FILE,
            new RuntimeException("Simulated transient failure")));

        BackupMemtableContext ctx = createContext(1000);
        File metaFile = newTargetFile("meta.json");
        ctx.downloadClosestMeta(metaFile);

        assertTrue(metaFile.exists());
        String content = new String(Files.readAllBytes(metaFile.toPath()));
        assertEquals("retried", content);
    }

    @Test(expected = RuntimeException.class)
    public void testDownloadClosestMeta_ThrowsAfterMaxRetries() throws Exception
    {
        createFakeMetaFile(900, "unreachable");

        // Inject 3 failures to exhaust all retries
        for (int i = 0; i < 3; i++)
        {
            fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Failure<>(
                AwsAsyncS3FakeBackup.Method.GET_OBJECT_AS_FILE,
                new RuntimeException("Persistent failure")));
        }

        BackupMemtableContext ctx = createContext(1000);
        ctx.downloadClosestMeta(newTargetFile("meta.json"));
    }
}
