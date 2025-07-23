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
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileReader;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class ResourcesMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(ResourcesMetrics.class);
    private static final MetricNameFactory factory = new DefaultNameFactory("Resources");
    private static final String SCHEDSTAT = System.getProperty("cassandra.schedstat_file", "/proc/schedstat");
    private static final String PSI = System.getProperty("cassandra.pressure_dir", "/proc/pressure");
    public static final SchedStatReader schedStatReader = new SchedStatReader(SCHEDSTAT);
    public static final PSIReader psiReader = new PSIReader(PSI);
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
    ResourcesMetrics::getDiskUtilization
    );

    public static double getDiskUtilization()
    {
        File root = new File(System.getProperty("cassandra.disk_usage_root", "/"));
        long total = root.toJavaIOFile().getTotalSpace();
        long free = root.toJavaIOFile().getUsableSpace();
        return total > 0 ? (double)(total - free) / total : 0.0;
    }
    public static final Gauge<Double> psiGauge = Metrics.register(
    factory.createMetricName("CpuPSI"),
    () -> psiReader.getMetrics().getPressure(PSIMeasurement.PressureType.CPU)
                   .map(PSIMeasurement::mediumAverage)
                   .orElse(0.0)
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

    public static class PSIReader
    {
        // Note: The PSI metrics support changing these times and providing custom keys.
        // This needs to be updated to match those keys if we want to change from the defaults.
        private static final String shortAverageKey = "avg10";
        private static final String mediumAverageKey = "avg60";
        private static final String longAverageKey = "avg300";
        private static final List<String> measureKeys = ImmutableList.of(shortAverageKey, mediumAverageKey, longAverageKey);

        private List<PSIMeasurement.PressureType> typesToRead;
        private final String path;

        public PSIReader(String path)
        {
            File file = new File(path);
            if (file.exists())
            {
                typesToRead = new ArrayList<>();
                this.path = path;
                for (PSIMeasurement.PressureType type : PSIMeasurement.PressureType.values())
                {
                    File pressureFile = new File(path, type.name().toLowerCase());
                    if (!pressureFile.exists())
                    {
                        logger.warn("Pressure metrics file {} does not exist", pressureFile.path());
                    }
                    else
                    {
                        typesToRead.add(type);
                    }
                }
            }
            else
            {
                this.path = null;
                logger.warn("Pressure metrics directory {} does not exist", path);
            }
        }

        public PSIMetrics getMetrics()
        {
            if (path == null)
                return new PSIMetrics(Collections.emptyList());

            List<PSIMeasurement> measurements = new ArrayList<>();
            for (PSIMeasurement.PressureType type : typesToRead) {
                File file = new File(path, type.name().toLowerCase());
                try (BufferedReader reader = new BufferedReader(new FileReader(file.absolutePath())))
                {
                    String line;
                    while ((line = reader.readLine()) != null)
                    {
                        if (line.startsWith("some"))
                        {
                            Map<String, Double> values = new HashMap<>();
                            for (String key : measureKeys) {
                                // The format is "some key1=0.0 key2=0.0 key3=0.0 total=123"
                                // Algorithm:
                                //   start index is the key. End index is the next space.
                                //   parse the value between the start and end index.
                                //   if either index is -1, skip the line.
                                String searchString = key + '=';
                                int startIndex = line.indexOf(searchString);
                                int endIndex = line.indexOf(' ', startIndex);
                                if (startIndex == -1 || endIndex == -1)
                                {
                                    // bailout— the format is invalid
                                    logger.error("Invalid format for line while reading key {} for pressure {}: {}", key, type, line);
                                    return new PSIMetrics(Collections.emptyList());
                                }
                                else
                                {
                                    String valueString = line.substring(startIndex + searchString.length(), endIndex);
                                    values.put(key, Double.parseDouble(valueString));
                                }
                            }
                            measurements.add(new PSIMeasurement(type,
                                                                values.get(shortAverageKey),
                                                                values.get(mediumAverageKey),
                                                                values.get(longAverageKey)));
                        }
                    }
                }
                catch (IOException e)
                {
                    logger.error("Error reading psi metrics from {}", path, e);
                }
            }

            return new PSIMetrics(measurements);
        }
    }


    public static class PSIMeasurement {
        public enum PressureType
        {
            CPU, MEMORY, IO
        }

        private final Double shortAverage;
        private final Double mediumAverage;
        private final Double longAverage;
        private final PressureType type;

        public PSIMeasurement(PressureType type, Double shortAverage, Double mediumAverage, Double longAverage)
        {
            this.type = type;
            this.shortAverage = shortAverage;
            this.mediumAverage = mediumAverage;
            this.longAverage = longAverage;
        }

        public Double shortAverage()
        {
            return shortAverage;
        }

        public Double mediumAverage()
        {
            return mediumAverage;
        }

        public Double longAverage()
        {
            return longAverage;
        }

        public PressureType type()
        {
            return type;
        }
    }

    public static class PSIMetrics
    {
        private final EnumMap<PSIMeasurement.PressureType, PSIMeasurement> measurements;

        public PSIMetrics(List<PSIMeasurement> pressures)
        {
            measurements = new EnumMap<>(PSIMeasurement.PressureType.class);
            for (PSIMeasurement pressure : pressures)
            {
                measurements.put(pressure.type(), pressure);
            }
        }

        public Optional<PSIMeasurement> getPressure(PSIMeasurement.PressureType type)
        {
            return Optional.ofNullable(measurements.get(type));
        }
    }
}
