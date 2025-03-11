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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ResourcesMetricsTest
{

    @Test
    public void test_CanBuildWithNonExistentPath_SchedulingDelayGauge()
    {
        ResourcesMetrics.SchedulingDelayGauge schedulingDelayGauge = new ResourcesMetrics.SchedulingDelayGauge("nonexistent_path");
        assertEquals(0.0, schedulingDelayGauge.getValue(), 0.001);
    }

    @Test
    public void test_basicCase_SchedulingDelayGauge()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_schedstat_simple.txt");
        String testFilePath = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.SchedulingDelayGauge schedulingDelayGauge = new ResourcesMetrics.SchedulingDelayGauge(testFilePath);

        double expectedAverageDelay = (10 + 20 + 30) / 3.0;

        assertEquals(expectedAverageDelay, schedulingDelayGauge.getValue(), 0.001);
    }

    @Test
    public void test_fullCase_SchedulingDelayGauge()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_schedstat_full.txt");
        String testFilePath = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.SchedulingDelayGauge schedulingDelayGauge = new ResourcesMetrics.SchedulingDelayGauge(testFilePath);

        double expectedAverageDelay = (533361260685D + 549142505865D + 522588242761D + 517028496925D) / 4.0;

        assertEquals(expectedAverageDelay, schedulingDelayGauge.getValue(), 0.001);
    }

    @Test
    public void test_AllZerosCase_SchedulingDelayGauge()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_schedstat_zeros.txt");
        String testFilePath = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.SchedulingDelayGauge schedulingDelayGauge = new ResourcesMetrics.SchedulingDelayGauge(testFilePath);

        double expectedAverageDelay = 0.0;

        assertEquals(expectedAverageDelay, schedulingDelayGauge.getValue(), 0.001);
    }

    @Test
    public void test_LargeNumbersCase_SchedulingDelayGauge()
    {
        URL resource = getClass().getClassLoader().getResource("netflix/metrics/mock_schedstat_large.txt");
        String testFilePath = Paths.get(resource.getPath()).toString();

        ResourcesMetrics.SchedulingDelayGauge schedulingDelayGauge = new ResourcesMetrics.SchedulingDelayGauge(testFilePath);

        double expectedAverageDelay = Long.MAX_VALUE;

        assertEquals(expectedAverageDelay, schedulingDelayGauge.getValue(), 0.001);
    }
}
