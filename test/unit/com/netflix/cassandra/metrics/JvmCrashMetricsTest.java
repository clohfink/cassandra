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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JvmCrashMetricsTest
{
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void test_emptySnapshot_whenDirectoryDoesNotExist() throws IOException
    {
        Path absent = tmp.getRoot().toPath().resolve("does-not-exist");
        JvmCrashMetrics.Snapshot s = JvmCrashMetrics.snapshot(absent.toString());
        assertEquals(0L, s.count);
        assertEquals(0L, s.bytes);
    }

    @Test
    public void test_emptySnapshot_whenDirectoryIsNull()
    {
        JvmCrashMetrics.Snapshot s = JvmCrashMetrics.snapshot(null);
        assertEquals(0L, s.count);
        assertEquals(0L, s.bytes);
    }

    @Test
    public void test_emptySnapshot_whenDirectoryEmpty() throws IOException
    {
        Path dir = tmp.newFolder().toPath();
        JvmCrashMetrics.Snapshot s = JvmCrashMetrics.snapshot(dir.toString());
        assertEquals(0L, s.count);
        assertEquals(0L, s.bytes);
    }

    @Test
    public void test_ignoresUnrelatedFiles() throws IOException
    {
        Path dir = tmp.newFolder().toPath();
        Files.write(dir.resolve("system.log"), new byte[]{1, 2, 3});
        Files.write(dir.resolve("gc.log"), new byte[]{1, 2, 3});
        Files.write(dir.resolve("hs_err_pid.log.bak"), new byte[]{1, 2, 3});
        Files.write(dir.resolve("not_hs_err_pid123.log"), new byte[]{1, 2, 3});
        JvmCrashMetrics.Snapshot s = JvmCrashMetrics.snapshot(dir.toString());
        assertEquals(0L, s.count);
        assertEquals(0L, s.bytes);
    }

    @Test
    public void test_countsAndSumsMatchingFiles() throws IOException
    {
        Path dir = tmp.newFolder().toPath();
        Files.write(dir.resolve("hs_err_pid1.log"), "abc".getBytes(StandardCharsets.UTF_8));
        Files.write(dir.resolve("hs_err_pid12345.log"), "12345".getBytes(StandardCharsets.UTF_8));
        Files.write(dir.resolve("hs_err_pid99999.log"), new byte[100]);
        // unrelated file should not be counted or summed
        Files.write(dir.resolve("system.log"), new byte[]{1, 2, 3});

        JvmCrashMetrics.Snapshot s = JvmCrashMetrics.snapshot(dir.toString());
        assertEquals(3L, s.count);
        assertEquals(3L + 5L + 100L, s.bytes);
    }

    @Test
    public void test_emptySnapshot_whenPathIsRegularFile() throws IOException
    {
        Path file = tmp.newFile().toPath();
        JvmCrashMetrics.Snapshot s = JvmCrashMetrics.snapshot(file.toString());
        assertEquals(0L, s.count);
        assertEquals(0L, s.bytes);
    }

    @Test
    public void test_hsErrDir_returnsNonNull()
    {
        // hsErrDir() inspects the running JVM. Whatever the JVM reports, the implementation must
        // always return a non-null directory (falling back to FALLBACK_HS_ERR_DIR).
        assertNotNull(JvmCrashMetrics.hsErrDir());
    }

    @Test
    public void test_snapshotCache_sharesSingleScanWithinWindow()
    {
        // The two gauges (count and bytes) read through the same cache, so the directory must be
        // scanned once per collection cycle, not once per gauge.
        AtomicInteger scans = new AtomicInteger();
        Supplier<JvmCrashMetrics.Snapshot> cache = JvmCrashMetrics.newSnapshotCache(() -> {
            scans.incrementAndGet();
            return new JvmCrashMetrics.Snapshot(2L, 7L);
        });

        JvmCrashMetrics.Snapshot count = cache.get();
        JvmCrashMetrics.Snapshot bytes = cache.get();

        assertEquals(1, scans.get());
        assertEquals(2L, count.count);
        assertEquals(7L, bytes.bytes);
    }
}
