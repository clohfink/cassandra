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
import java.util.regex.Pattern;

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
    private static final String PATH = System.getProperty("cassandra.schedstat_file", "/proc/schedstat");
    public static final Gauge<Double> schedulingDelay = Metrics.register(
    factory.createMetricName("CpuDelay"),
    new SchedulingDelayGauge(PATH)
    );
    public static final Gauge<Double> disk = Metrics.register(
    factory.createMetricName("DiskUtil"),
    () -> DiskUsageMonitor.instance.getDiskUsage()
    );

    public static class SchedulingDelayGauge implements Gauge<Double>
    {
        private static final Pattern PATTERN = Pattern.compile("\\s+");
        private final String path;

        public SchedulingDelayGauge(String path)
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

        /**
         * Returns the average scheduling delay in nanoseconds.
         * <p>
         * Excerpt from kernel documentation:
         * <pre>
         * CPU statistics
         * --------------
         * cpu<N> 1 2 3 4 5 6 7 8 9
         * ...
         * Next three are statistics describing scheduling latency:
         *      7) sum of all time spent running by tasks on this processor (in jiffies)
         *      8) sum of all time spent waiting to run by tasks on this processor (in
         *         jiffies)
         *      9) # of timeslices run on this cpu
         * </pre>
         *
         * We are reading the 8th statistic from the file.
         */
        @Override
        public Double getValue()
        {
            if (path == null)
                return 0.0;

            double sum = 0;
            int count = 0;

            try (BufferedReader reader = new BufferedReader(new FileReader(path)))
            {
                String line;
                while ((line = reader.readLine()) != null)
                {
                    if (line.startsWith("cpu"))
                    {
                        String[] parts = PATTERN.split(line.trim());
                        if (parts.length > 8)
                        {
                            try
                            {
                                long delay = Long.parseLong(parts[8]);
                                sum += delay;
                                count++;
                            }
                            catch (NumberFormatException e)
                            {
                                // Ignore lines with an invalid number
                            }
                        }
                    }
                }
            }
            catch (IOException e)
            {
                logger.error("Error reading scheduling delay from {}", path, e);
            }

            return (count == 0) ? 0.0 : sum / count;
        }
    }
}
