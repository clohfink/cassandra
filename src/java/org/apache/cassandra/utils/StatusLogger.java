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
package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cache.*;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class StatusLogger
{
    private static final Logger logger = LoggerFactory.getLogger(StatusLogger.class);
    private static final ReentrantLock busyMonitor = new ReentrantLock();
    private static final Pattern ALLOW_FILTERING = Pattern.compile("ALLOW FILTERING$");

    public static void log()
    {
        // avoid logging more than once at the same time. throw away any attempts to log concurrently, as it would be
        // confusing and noisy for operators - and don't bother logging again, immediately as it'll just be the same data
        if (busyMonitor.tryLock())
        {
            try
            {
                logStatus();
            }
            finally
            {
                busyMonitor.unlock();
            }
        }
        else
        {
            logger.trace("StatusLogger is busy");
        }
    }

    private static void logStatus()
    {
        // everything from o.a.c.concurrent
        logger.info(String.format("%-28s%10s%10s%15s%10s%18s", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All Time Blocked"));

        for (ThreadPoolMetrics tpool : CassandraMetricsRegistry.Metrics.allThreadPoolMetrics())
        {
            logger.info(String.format("%-28s%10s%10s%15s%10s%18s",
                                      tpool.poolName,
                                      tpool.activeTasks.getValue(),
                                      tpool.pendingTasks.getValue(),
                                      tpool.completedTasks.getValue(),
                                      tpool.currentBlocked.getCount(),
                                      tpool.totalBlocked.getCount()));
        }

        // one offs
        logger.info(String.format("%-25s%10s%10s",
                                  "CompactionManager", CompactionManager.instance.getActiveCompactions(), CompactionManager.instance.getPendingTasks()));
        int pendingLargeMessages = 0;
        for (int n : MessagingService.instance().getLargeMessagePendingTasks().values())
        {
            pendingLargeMessages += n;
        }
        int pendingSmallMessages = 0;
        for (int n : MessagingService.instance().getSmallMessagePendingTasks().values())
        {
            pendingSmallMessages += n;
        }
        logger.info(String.format("%-25s%10s%10s",
                                  "MessagingService", "n/a", pendingLargeMessages + "/" + pendingSmallMessages));

        // Global key/row cache information
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = CacheService.instance.keyCache;
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = CacheService.instance.rowCache;

        int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();
        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "Cache Type", "Size", "Capacity", "KeysToSave"));
        logger.info(String.format("%-25s%10s%25s%25s",
                                  "KeyCache",
                                  keyCache.weightedSize(),
                                  keyCache.getCapacity(),
                                  keyCacheKeysToSave == Integer.MAX_VALUE ? "all" : keyCacheKeysToSave));

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "KeyCache",
                                  keyCache.weightedSize(),
                                  keyCache.getCapacity(),
                                  keyCacheKeysToSave == Integer.MAX_VALUE ? "all" : keyCacheKeysToSave));

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "RowCache",
                                  rowCache.weightedSize(),
                                  rowCache.getCapacity(),
                                  rowCacheKeysToSave == Integer.MAX_VALUE ? "all" : rowCacheKeysToSave));

        // current queries
        List<DebuggableTask.RunningDebuggableTask> tasks = SharedExecutorPool.SHARED.runningTasks();
        if (!tasks.isEmpty()) {
            logger.info("Longest Running Tasks by Stage:");

            // Collect tasks with valid `hasTask()`
            List<DebuggableTask.RunningDebuggableTask> validTasks = new ArrayList<>();
            for (DebuggableTask.RunningDebuggableTask task : tasks) {
                if (task.hasTask()) {
                    validTasks.add(task);
                }
            }

            // Group tasks by the substring from the beginning of the thread ID up to the last "-"
            Map<String, List<DebuggableTask.RunningDebuggableTask>> groupedTasks = getTasksByStage(validTasks);

            // For each group, sort by `queuedMicros` in descending order and take the top 5
            for (Map.Entry<String, List<DebuggableTask.RunningDebuggableTask>> entry : groupedTasks.entrySet()) {
                String threadGroup = entry.getKey();
                List<DebuggableTask.RunningDebuggableTask> taskList = worstTasksOf(entry);

                // Take the top 5 tasks
                List<DebuggableTask.RunningDebuggableTask> topTasks = new ArrayList<>();
                for (int i = 0; i < Math.min(taskList.size(), 5); i++) {
                    topTasks.add(taskList.get(i));
                }

                if (topTasks.isEmpty()) {
                    continue;
                }

                // Print the thread group
                logger.info(String.format("Stage: %s", threadGroup));
                logger.info(String.format("%-30s%-15s%-15s %s", "Thread", "Queued_Micros", "Running_Micros", "Query"));

                // Print the top 5 tasks from each group
                for (DebuggableTask.RunningDebuggableTask task : topTasks) {
                    long creationTimeNanos = task.creationTimeNanos();
                    long startTimeNanos = task.startTimeNanos();
                    long now = approxTime.now();

                    long queuedMicros = NANOSECONDS.toMicros(Math.max((startTimeNanos > 0 ? startTimeNanos : now) - creationTimeNanos, 0));
                    long runningMicros = startTimeNanos > 0 ? NANOSECONDS.toMicros(now - startTimeNanos) : 0;

                    String taskDescription = ALLOW_FILTERING.matcher(task.description()).replaceAll("");
                    String truncatedTask = taskDescription.length() > 128 ? taskDescription.substring(0, 125) + "...[" + taskDescription.length() + ']' : taskDescription;

                    logger.info(String.format("%-30s%-15s%-15s %s", task.threadId(), queuedMicros, runningMicros, truncatedTask));
                }
            }
        }
    }

    @VisibleForTesting
    static Map<String, List<DebuggableTask.RunningDebuggableTask>> getTasksByStage(List<DebuggableTask.RunningDebuggableTask> validTasks)
    {
        Map<String, List<DebuggableTask.RunningDebuggableTask>> groupedTasks = new HashMap<>();
        for (DebuggableTask.RunningDebuggableTask task : validTasks) {
            String threadId = task.threadId();
            int lastDashIndex = threadId.lastIndexOf('-');
            String threadGroup = (lastDashIndex >= 0) ? threadId.substring(0, lastDashIndex) : threadId;

            // Add task to its group
            groupedTasks.computeIfAbsent(threadGroup, k -> new ArrayList<>()).add(task);
        }
        return groupedTasks;
    }

    @VisibleForTesting
    static List<DebuggableTask.RunningDebuggableTask> worstTasksOf(Map.Entry<String, List<DebuggableTask.RunningDebuggableTask>> entry)
    {
        List<DebuggableTask.RunningDebuggableTask> taskList = entry.getValue();

        taskList.sort(new Comparator<DebuggableTask.RunningDebuggableTask>() {
            @Override
            public int compare(DebuggableTask.RunningDebuggableTask t1, DebuggableTask.RunningDebuggableTask t2) {
                long now = approxTime.now();
                return Long.compare(calculateTime(t2, now), calculateTime(t1, now));
            }
        });
        return taskList;
    }

    private static long calculateTime(DebuggableTask.RunningDebuggableTask task, long now) {
        long startTimeNanos = task.startTimeNanos();
        long creationTimeNanos = task.creationTimeNanos();

        long startTime = (startTimeNanos > 0) ? startTimeNanos : now;
        long timeDiffNanos = Math.max(startTime - creationTimeNanos, 0);
        return NANOSECONDS.toMicros(timeDiffNanos);
    }
}
