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

package com.netflix.cassandra.db.virtual;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingWindowReservoir;
import com.netflix.cassandra.metrics.ResourcesMetrics;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;

public class ResourcesTable extends AbstractVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(ResourcesTable.class);
    private static final String NAME = "type";
    private static final String VALUE = "value";

    private static final ScheduledExecutorService scheduler = ExecutorFactory.Global.executorFactory().scheduled("ResourceUtilMonitor");

    public static final String TABLE_NAME = "resource_util";
    private final CpuUsageMonitor monitor;
    private final ThreadsWaitingMonitor threadsWaitingMonitor;

    ResourcesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("current system utilization")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(VALUE, DoubleType.instance)
                           .build());
        monitor = new CpuUsageMonitor();
        monitor.startMonitoring();
        threadsWaitingMonitor = new ThreadsWaitingMonitor();
        threadsWaitingMonitor.startMonitoring();
    }

    public static double clampAndRound(double value)
    {
        return round(clamp(value));
    }

    private static double clamp(double value)
    {
        // Clamp the value to the range [0, 1]
        return Math.max(0.0, Math.min(1.0, value));
    }

    private static double round(double value)
    {
        // Multiply by 100, round up, then divide by 100 to get 100ths precision
        return Math.ceil(value * 100.0) / 100.0;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        double cpu = clampAndRound(monitor.getAverageCpuUsage() / 100.0);
        double disk = clampAndRound(DiskUsageMonitor.instance.getDiskUsage());
        double threadsWaiting = threadsWaitingMonitor.getAverageThreadsWaiting();

        ResourcesMetrics.PSIMetrics psiMetrics = ResourcesMetrics.psiReader.getMetrics();
        double cpuPressureShort = psiMetrics.getPressure(ResourcesMetrics.PSIMeasurement.PressureType.CPU)
                                            .map(ResourcesMetrics.PSIMeasurement::shortAverage).orElse(0.0);
        double cpuPressureMed = psiMetrics.getPressure(ResourcesMetrics.PSIMeasurement.PressureType.CPU)
                                          .map(ResourcesMetrics.PSIMeasurement::mediumAverage).orElse(0.0);
        double cpuPressureLong = psiMetrics.getPressure(ResourcesMetrics.PSIMeasurement.PressureType.CPU)
                                           .map(ResourcesMetrics.PSIMeasurement::longAverage).orElse(0.0);

        result.row("compute").column(VALUE, cpu);
        result.row("disk").column(VALUE, disk);
        result.row("threadsWaiting").column(VALUE, threadsWaiting);
        result.row("cpuPressureShort").column(VALUE, cpuPressureShort);
        result.row("cpuPressureMed").column(VALUE, cpuPressureMed);
        result.row("cpuPressureLong").column(VALUE, cpuPressureLong);

        return result;
    }

    public static class CpuUsageMonitor
    {
        // Number of samples in the sliding window (one per second for 60 seconds)
        private static final int WINDOW_SIZE = 60;

        private final OperatingSystemMXBean osBean;
        private final Histogram cpuUsageHistogram;
        private volatile double averageCpuUsage;

        public CpuUsageMonitor()
        {
            osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            cpuUsageHistogram = new Histogram(new SlidingWindowReservoir(WINDOW_SIZE));
            averageCpuUsage = 0.0;
        }

        public void startMonitoring()
        {
            // Every second, sample the system load average, update the histogram, and update the average.
            scheduler.scheduleWithFixedDelay(() -> {
                double cpuLoad = osBean.getSystemLoadAverage();
                int cores = Runtime.getRuntime().availableProcessors();
                // Normalize the load average by dividing by the number of CPU cores.
                double normalizedLoad = cpuLoad / cores;
                // Clamp the normalized load to [0, 1].
                normalizedLoad = Math.min(Math.max(normalizedLoad, 0.0), 1.0);
                cpuUsageHistogram.update((long) (normalizedLoad * 100));
                averageCpuUsage = cpuUsageHistogram.getSnapshot().getMean();
            }, 0, 1, TimeUnit.SECONDS);
        }

        public double getAverageCpuUsage()
        {
            return averageCpuUsage;
        }

        public void stopMonitoring()
        {
            scheduler.shutdown();
        }
    }

    public static class ThreadsWaitingMonitor
    {
        static final int WINDOW_SIZE = 10;
        final List<ResourcesMetrics.SchedStatMetrics> buffer = new ArrayList<>();
        ResourcesMetrics.SchedStatMetrics previousMeasure;

        public ThreadsWaitingMonitor()
        {
            previousMeasure = ResourcesMetrics.schedStatReader.getMetrics();
        }

        public void startMonitoring()
        {
            scheduler.scheduleWithFixedDelay(() -> {
                ResourcesMetrics.SchedStatMetrics currentMeasure = ResourcesMetrics.schedStatReader.getMetrics();
                ResourcesMetrics.SchedStatMetrics diff = new ResourcesMetrics.SchedStatMetrics(
                currentMeasure.coreAveragedtotalDelay() - previousMeasure.coreAveragedtotalDelay(),
                currentMeasure.coreAveragedtotalRunningTime() - previousMeasure.coreAveragedtotalRunningTime()
                );
                synchronized (buffer)
                {
                    buffer.add(diff);
                    if (buffer.size() > WINDOW_SIZE)
                    {
                        buffer.remove(0);
                    }
                }
                previousMeasure = currentMeasure;
            }, 0, 1, TimeUnit.MINUTES);
        }

        public double getAverageThreadsWaiting()
        {
            synchronized (buffer)
            {
                if (buffer.isEmpty())
                {
                    return 0;
                }
                double totalDelay = 0;
                double totalRunningTime = 0;

                for (ResourcesMetrics.SchedStatMetrics measure : buffer)
                {
                    totalDelay += measure.coreAveragedtotalDelay();
                    totalRunningTime += measure.coreAveragedtotalRunningTime();
                }
                return totalRunningTime == 0 ? 0 :
                       round(((totalDelay / totalRunningTime)));
            }
        }
    }
}
