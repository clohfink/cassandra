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

package org.apache.cassandra.distributed.test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.InstanceClassLoader;

import static org.junit.Assert.assertEquals;

/**
 * Test for detecting thread leaks by repeatedly creating and shutting down clusters.
 * This test helps identify threads that are not properly terminated during cluster shutdown.
 */
public class ThreadLeakTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ThreadLeakTest.class);

    // Set to true to enable periodic heap dumps during testNodeRestartThreadLeaks
    private static final boolean ENABLE_PERIODIC_HEAP_DUMPS = false;
    // Interval in seconds between heap dumps (default: 10 seconds)
    private static final int HEAP_DUMP_INTERVAL_SECONDS = Integer.getInteger("test.threadleak.dump.interval", 30);
    // Directory to write heap dumps (default: build/test/heapdumps)
    private static final String HEAP_DUMP_DIR = System.getProperty("test.threadleak.dump.dir", "build/test/heapdumps");

    private ScheduledExecutorService heapDumpScheduler;
    private AtomicInteger heapDumpCounter = new AtomicInteger(0);

    /**
     * Creates and shuts down a cluster multiple times to detect thread leaks.
     * The test captures thread snapshots before and after cluster operations
     * and validates that all instance classloader threads are properly cleaned up.
     */
    @Test
    public void testRepeatedClusterStartupShutdownForThreadLeaks() throws Exception
    {
        int iterations = 10;
        int nodeCount = 1;

        // Start periodic heap dumps if enabled
        if (ENABLE_PERIODIC_HEAP_DUMPS)
        {
            startPeriodicHeapDumps();
        }
        Map<String, Integer> baselineThreadCounts = captureThreadGroupCounts();
        Set<Thread> initialThreads = Thread.getAllStackTraces().keySet();

        logger.info("Starting thread leak test with {} iterations and {} nodes per cluster", iterations, nodeCount);
        logger.info("Baseline thread groups: {}", baselineThreadCounts);

        for (int i = 0; i < iterations; i++)
        {
            logger.info("Iteration {}/{}: Creating cluster", i + 1, iterations);

            try (Cluster cluster = init(Cluster.build(nodeCount).start()))
            {
                // Perform some basic operations to ensure threads are created
                cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                                     " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + nodeCount + "}");
                cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (id int primary key, v int)");

                // Insert some data to exercise various thread pools
                for (int j = 0; j < 10; j++)
                {
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, v) VALUES (?, ?)",
                                                   ConsistencyLevel.ALL,
                                                   j, j);
                }

                // Read data to exercise read paths
                cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE id = ?",
                                               ConsistencyLevel.ONE,
                                               0);
            }

            logger.info("Iteration {}/{}: Cluster shutdown complete", i + 1, iterations);

            // Give threads time to clean up
            Thread.sleep(500);

            // Check for leaked threads
            checkForLeakedThreads(i + 1);
        }

        // Final check after all iterations
        logger.info("All iterations complete. Performing final thread leak check...");

        // Give any lingering cleanup tasks time to complete
        Thread.sleep(1000);

        Set<Thread> finalThreads = Thread.getAllStackTraces().keySet();
        Map<String, Integer> finalThreadCounts = captureThreadGroupCounts();

        // Report on thread group changes
        logger.info("Final thread groups: {}", finalThreadCounts);
        for (Map.Entry<String, Integer> entry : finalThreadCounts.entrySet())
        {
            String group = entry.getKey();
            int finalCount = entry.getValue();
            int baselineCount = baselineThreadCounts.getOrDefault(group, 0);
            int delta = finalCount - baselineCount;

            if (delta > 0)
            {
                logger.warn("Thread group '{}' increased by {} threads (baseline: {}, final: {})",
                            group, delta, baselineCount, finalCount);
            }
        }

        // Check for threads with instance classloaders (these are definite leaks)
        List<Thread> leakedInstanceThreads = findThreadsWithInstanceClassLoader(finalThreads);
        if (!leakedInstanceThreads.isEmpty())
        {
            logger.error("Found {} threads with InstanceClassLoader after all clusters shutdown:",
                         leakedInstanceThreads.size());
            for (Thread t : leakedInstanceThreads)
            {
                logger.error("  Leaked thread: {} (group: {}, daemon: {}, state: {})",
                             t.getName(),
                             t.getThreadGroup() != null ? t.getThreadGroup().getName() : "null",
                             t.isDaemon(),
                             t.getState());
                StackTraceElement[] stack = t.getStackTrace();
                if (stack.length > 0)
                {
                    logger.error("    Top of stack: {}", stack[0]);
                }
            }
        }

        assertEquals("Found threads with InstanceClassLoader after cluster shutdown",
                     0, leakedInstanceThreads.size());
    }

    /**
     * Captures a snapshot of current thread groups and their thread counts.
     */
    private Map<String, Integer> captureThreadGroupCounts()
    {
        Map<String, Integer> counts = new HashMap<>();
        Set<Thread> threads = Thread.getAllStackTraces().keySet();

        for (Thread t : threads)
        {
            ThreadGroup group = t.getThreadGroup();
            if (group != null)
            {
                String groupName = group.getName();
                counts.put(groupName, counts.getOrDefault(groupName, 0) + 1);
            }
        }

        return counts;
    }

    /**
     * Finds all threads that have an InstanceClassLoader as their context classloader.
     * These threads represent leaked instance threads that should have been cleaned up.
     */
    private List<Thread> findThreadsWithInstanceClassLoader(Set<Thread> threads)
    {
        List<Thread> leaked = new ArrayList<>();

        for (Thread t : threads)
        {
            ClassLoader cl = t.getContextClassLoader();
            if (cl instanceof InstanceClassLoader)
            {
                leaked.add(t);
            }
        }

        return leaked;
    }

    /**
     * Checks for leaked threads and logs warnings if any are found.
     */
    private void checkForLeakedThreads(int checkpointId)
    {
        Set<Thread> currentThreads = Thread.getAllStackTraces().keySet();
        List<Thread> leaked = findThreadsWithInstanceClassLoader(currentThreads);

        if (!leaked.isEmpty())
        {
            logger.warn("Checkpoint {}: Found {} threads with InstanceClassLoader:",
                        checkpointId, leaked.size());
            for (Thread t : leaked)
            {
                logger.warn("  Thread: {} (group: {}, state: {})",
                            t.getName(),
                            t.getThreadGroup() != null ? t.getThreadGroup().getName() : "null",
                            t.getState());
            }
        }
        else
        {
            logger.info("Checkpoint {}: No leaked instance threads detected", checkpointId);
        }
    }

    /**
     * Starts periodic heap dump collection in the background.
     */
    private void startPeriodicHeapDumps()
    {
        // Create output directory
        File dumpDir = new File(HEAP_DUMP_DIR);
        if (!dumpDir.exists() && !dumpDir.mkdirs())
        {
            logger.error("Failed to create heap dump directory: {}", dumpDir.getAbsolutePath());
            return;
        }

        logger.info("Starting periodic heap dumps every {} seconds to directory: {}",
                    HEAP_DUMP_INTERVAL_SECONDS, dumpDir.getAbsolutePath());

        heapDumpCounter.set(0);
        heapDumpScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "HeapDumpScheduler");
            t.setDaemon(true);
            return t;
        });

        heapDumpScheduler.scheduleAtFixedRate(() -> {
            try
            {
                captureHeapDump();
            }
            catch (Exception e)
            {
                logger.error("Error capturing heap dump", e);
            }
        }, HEAP_DUMP_INTERVAL_SECONDS, HEAP_DUMP_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Stops periodic heap dump collection.
     */
    private void stopPeriodicHeapDumps()
    {
        if (heapDumpScheduler != null)
        {
            logger.info("Stopping periodic heap dumps. Total dumps captured: {}", heapDumpCounter.get());
            heapDumpScheduler.shutdown();
            try
            {
                if (!heapDumpScheduler.awaitTermination(5, TimeUnit.SECONDS))
                {
                    heapDumpScheduler.shutdownNow();
                }
            }
            catch (InterruptedException e)
            {
                heapDumpScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Captures a heap dump using HotSpot MXBean.
     */
    private void captureHeapDump()
    {
        int dumpNumber = heapDumpCounter.incrementAndGet();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
        String timestamp = dateFormat.format(new Date());
        String filename = String.format("heapdump-%s-%03d.hprof", timestamp, dumpNumber);
        File dumpFile = new File(HEAP_DUMP_DIR, filename);

        // Log memory info before dumping
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();

        logger.info("Heap dump #{}: Memory before dump - used={}MB, free={}MB, total={}MB, max={}MB",
                    dumpNumber,
                    usedMemory / (1024 * 1024), freeMemory / (1024 * 1024),
                    totalMemory / (1024 * 1024), maxMemory / (1024 * 1024));

        try
        {
            // Use HotSpot diagnostic MXBean to dump heap
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName mxbeanName = new ObjectName("com.sun.management:type=HotSpotDiagnostic");

            // Invoke dumpHeap method: dumpHeap(String outputFile, boolean live)
            // live=true means only dump live objects (triggers GC first)
            server.invoke(mxbeanName, "dumpHeap",
                          new Object[]{ dumpFile.getAbsolutePath(), true },
                          new String[]{ "java.lang.String", "boolean" });

            long dumpSize = dumpFile.length();
            logger.info("Heap dump #{} written to: {} (size: {}MB)",
                        dumpNumber, dumpFile.getAbsolutePath(), dumpSize / (1024 * 1024));
        }
        catch (Exception e)
        {
            logger.error("Failed to capture heap dump #{}", dumpNumber, e);
        }
    }
}
