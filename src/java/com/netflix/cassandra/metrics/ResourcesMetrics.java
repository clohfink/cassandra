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

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileReader;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class ResourcesMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(ResourcesMetrics.class);
    private static final MetricNameFactory factory = new DefaultNameFactory("Resources");
    private static final String SCHEDSTAT = System.getProperty("cassandra.schedstat_file", "/proc/schedstat");
    public static final SchedStatReader schedStatReader = new SchedStatReader(SCHEDSTAT);
    public static final Gauge<Long> schedulingDelay = Metrics.register(
    factory.createMetricName("CpuDelay"),
    new SchedulingDelayGauge(schedStatReader)
    );
    public static final Gauge<Long> runningTime = Metrics.register(
    factory.createMetricName("CpuRunningTime"),
    new RunningTimeGauge(schedStatReader)
    );
    public static final Gauge<Double> disk = Metrics.register(
    factory.createMetricName("DiskUtil"),
    () -> DiskUsageMonitor.instance.getDiskUsage()
    );

    public static class SchedulingDelayGauge implements Gauge<Long>
    {
        private final SchedStatReader reader;

        public SchedulingDelayGauge(SchedStatReader reader)
        {
            this.reader = reader;
        }

        @Override
        public Long getValue()
        {
            return reader.getMetrics().coreAveragedtotalDelay();
        }
    }

    public static class RunningTimeGauge implements Gauge<Long>
    {
        private final SchedStatReader reader;

        public RunningTimeGauge(SchedStatReader reader)
        {
            this.reader = reader;
        }

        @Override
        public Long getValue()
        {
            return reader.getMetrics().coreAveragedtotalRunningTime();
        }
    }


    /**
     * Returns a metric, by index, from scheduling latency statistics.
     * <p>
     * Excerpt from kernel documentation:
     * <pre>
     * CPU statistics
     * --------------
     * cpu<N> 1 2 3 4 5 6 7 8 9
     * ...
     * Next three are statistics describing scheduling latency:
     *      7) sum of all time spent running by tasks on this processor (in nanoseconds)
     *      8) sum of all time spent waiting to run by tasks on this processor (in
     *         nanoseconds)
     *      9) # of timeslices run on this cpu
     * </pre>
     * <p>
     * We are reading the 7th and 8th statistics from the file.
     */
    public static class SchedStatReader
    {
        private final Pattern PATTERN = Pattern.compile("\\s+");
        private final String path;
        private static final List<Integer> targetIndicies = ImmutableList.of(7, 8);

        public SchedStatReader(String path)
        {
            File file = new File(path);
            if (file.exists())
                this.path = path;
            else
            {
                this.path = null;
                logger.warn("Scheduling delay metrics file {} does not exist", path);
            }
        }

        public SchedStatMetrics getMetrics()
        {
            if (path == null)
                return new SchedStatMetrics(0, 0);

            long[] sums = new long[targetIndicies.size()];
            long[] counts = new long[targetIndicies.size()];

            try (BufferedReader reader = new BufferedReader(new FileReader(path)))
            {
                String line;
                while ((line = reader.readLine()) != null)
                {
                    if (line.startsWith("cpu"))
                    {
                        String[] parts = PATTERN.split(line.trim());
                        for (int i = 0; i < targetIndicies.size(); i++)
                        {
                            int index = targetIndicies.get(i);
                            if (parts.length > index)
                            {
                                try
                                {
                                    sums[i] += Long.parseLong(parts[index]);
                                    counts[i]++;
                                }
                                catch (NumberFormatException e)
                                {
                                    // Ignore lines with an invalid number
                                }
                            }
                        }
                    }
                }
            }
            catch (IOException e)
            {
                logger.error("Error reading scheduling delay from {}", path, e);
            }
            long coreAveragedTotalRunningTime = safeDivide(sums[0], counts[0]);
            long coreAveragedTotalDelay = safeDivide(sums[1], counts[1]);
            return new SchedStatMetrics(coreAveragedTotalRunningTime, coreAveragedTotalDelay);
        }

        private long safeDivide(long dividend, long divisor)
        {
            return divisor == 0 ? 0 : dividend / divisor;
        }
    }

    public static class SchedStatMetrics
    {
        private final long coreAveragedtotalDelay;
        private final long coreAveragedtotalRunningTime;

        public SchedStatMetrics(long coreAveragedTotalRunningTime, long coreAveragedtotalDelay)
        {
            this.coreAveragedtotalRunningTime = coreAveragedTotalRunningTime;
            this.coreAveragedtotalDelay = coreAveragedtotalDelay;
        }

        public long coreAveragedtotalRunningTime()
        {
            return coreAveragedtotalRunningTime;
        }

        public long coreAveragedtotalDelay()
        {
            return coreAveragedtotalDelay;
        }
    }
}
