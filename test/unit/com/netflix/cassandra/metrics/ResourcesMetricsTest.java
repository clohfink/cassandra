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

import java.net.URL;
import java.nio.file.Paths;
import java.util.Optional;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ResourcesMetricsTest
{

    @Test
    public void test_CanBuildWithNonExistentPath_SchedulingDelayGauge()
    {
        ResourcesMetrics.SchedStatReader reader = new ResourcesMetrics.SchedStatReader("nonexistent_path");
        assertEquals(0, reader.getMetrics().coreAveragedtotalDelay());
        assertEquals(0, reader.getMetrics().coreAveragedtotalRunningTime());
    }

    @Test
    public void test_basicCase_SchedulingDelayGauge()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_schedstat_simple.txt");
        String testFilePath = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.SchedStatReader reader = new ResourcesMetrics.SchedStatReader(testFilePath);

        long expectedAverageDelay = (10 + 20 + 30) / 3;
        long expectedAverageRunningTime = (100 + 200 + 300) / 3;

        assertEquals(expectedAverageDelay, reader.getMetrics().coreAveragedtotalDelay());
        assertEquals(expectedAverageRunningTime, reader.getMetrics().coreAveragedtotalRunningTime());
    }

    @Test
    public void test_fullCase_SchedulingDelayGauge()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_schedstat_full.txt");
        String testFilePath = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.SchedStatReader reader = new ResourcesMetrics.SchedStatReader(testFilePath);

        long expectedAverageDelay = (533361260685L + 549142505865L + 522588242761L + 517028496925L) / 4;
        long expectedAverageRunningTime = (12278208226386L + 12388964511605L + 12457177189225L + 12427090126794L) / 4;

        assertEquals(expectedAverageDelay, reader.getMetrics().coreAveragedtotalDelay());
        assertEquals(expectedAverageRunningTime, reader.getMetrics().coreAveragedtotalRunningTime());
    }

    @Test
    public void test_AllZerosCase_SchedulingDelayGauge()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_schedstat_zeros.txt");
        String testFilePath = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.SchedStatReader reader = new ResourcesMetrics.SchedStatReader(testFilePath);

        long expectedAverageDelay = 0;
        long expectedAverageRunningTime = 0;

        assertEquals(expectedAverageDelay, reader.getMetrics().coreAveragedtotalDelay());
        assertEquals(expectedAverageRunningTime, reader.getMetrics().coreAveragedtotalRunningTime());
    }

    @Test
    public void test_LargeNumbersCase_SchedulingDelayGauge()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_schedstat_large.txt");
        String testFilePath = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.SchedStatReader reader = new ResourcesMetrics.SchedStatReader(testFilePath);

        // This is the limit of how high these stats can go— long.MAX_VALUE / num cores
        long expectedAverageDelay = Long.MAX_VALUE / 3;
        long expectedAverageRunningTime = Long.MAX_VALUE / 3;

        assertEquals(expectedAverageDelay, reader.getMetrics().coreAveragedtotalDelay());
        assertEquals(expectedAverageRunningTime, reader.getMetrics().coreAveragedtotalRunningTime());
    }

    @Test
    public void test_PressureMetrics_DoNotExist()
    {
        ResourcesMetrics.PSIReader reader = new ResourcesMetrics.PSIReader("nonexistent_path");
        assertNotNull(reader.getMetrics());
        assertFalse(reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.CPU).isPresent());
        assertFalse(reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.MEMORY).isPresent());
        assertFalse(reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.IO).isPresent());
    }

    @Test
    public void test_PressureMetrics_NormalCase()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_pressure");
        String dir = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.PSIReader reader = new ResourcesMetrics.PSIReader(dir);
        assertNotNull(reader.getMetrics());
        Optional<ResourcesMetrics.PSIMeasurement> cpu = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.CPU);
        Optional<ResourcesMetrics.PSIMeasurement> memory = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.MEMORY);
        Optional<ResourcesMetrics.PSIMeasurement> io = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.IO);

        assertTrue(cpu.isPresent());
        assertTrue(memory.isPresent());
        assertTrue(io.isPresent());

        for (ResourcesMetrics.PSIMeasurement measurement : new ResourcesMetrics.PSIMeasurement[]{cpu.get(), memory.get(), io.get()})
        {
            assertEquals(1.11, measurement.shortAverage(), 0.001);
            assertEquals(2.22, measurement.mediumAverage(), 0.001);
            assertEquals(3.33, measurement.longAverage(), 0.001);
        }
    }

    @Test
    public void test_PressureMetrics_OnlyCPU()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_pressure_cpu_only");
        String dir = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.PSIReader reader = new ResourcesMetrics.PSIReader(dir);
        assertNotNull(reader.getMetrics());
        Optional<ResourcesMetrics.PSIMeasurement> cpu = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.CPU);
        Optional<ResourcesMetrics.PSIMeasurement> memory = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.MEMORY);
        Optional<ResourcesMetrics.PSIMeasurement> io = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.IO);

        assertTrue(cpu.isPresent());
        assertFalse(memory.isPresent());
        assertFalse(io.isPresent());
    }

    @Test
    public void test_PressureMetrics_Malformed()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_pressure_malformed");
        String dir = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.PSIReader reader = new ResourcesMetrics.PSIReader(dir);
        assertNotNull(reader.getMetrics());
        Optional<ResourcesMetrics.PSIMeasurement> cpu = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.CPU);
        Optional<ResourcesMetrics.PSIMeasurement> memory = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.MEMORY);
        Optional<ResourcesMetrics.PSIMeasurement> io = reader.getMetrics().getPressure(ResourcesMetrics.PSIMeasurement.PressureType.IO);

        assertFalse(cpu.isPresent());
        assertFalse(memory.isPresent());
        assertFalse(io.isPresent());
    }
}
