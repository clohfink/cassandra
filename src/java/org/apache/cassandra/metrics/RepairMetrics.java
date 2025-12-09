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

package org.apache.cassandra.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;

import org.apache.cassandra.repair.state.CoordinatorState;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class RepairMetrics
{
    public static final String TYPE_NAME = "Repair";
    public static final Counter previewFailures = Metrics.counter(DefaultNameFactory.createMetricName(TYPE_NAME, "PreviewFailures", null));

    private static final Map<TimeUUID, CassandraMetricsRegistry.MetricName> registeredRepairMetrics = new ConcurrentHashMap<>();

    /**
     * Register a per-repair elapsed time metric for a specific repair session.
     * This should be called when a repair starts.
     *
     * @param state the coordinator state for the repair
     */
    public static void registerRepairElapsedMetric(CoordinatorState state)
    {
        String scope = String.format("cmd-%d-keyspace-%s", state.cmd, state.keyspace);
        CassandraMetricsRegistry.MetricName metricName = DefaultNameFactory.createMetricName(TYPE_NAME, "RepairElapsedSec", scope);

        Gauge<Integer> gauge = () -> {
            if (state.isComplete())
            {
                return 0;
            }
            return (int) TimeUnit.MILLISECONDS.toSeconds(Clock.Global.currentTimeMillis() - state.getInitializedAtMillis());
        };

        Metrics.register(metricName, gauge);
        registeredRepairMetrics.put(state.id, metricName);
    }

    /**
     * Unregister the per-repair elapsed time metric when a repair completes.
     *
     * @param repairId the repair session ID
     */
    public static void unregisterRepairElapsedMetric(TimeUUID repairId)
    {
        CassandraMetricsRegistry.MetricName metricName = registeredRepairMetrics.remove(repairId);
        if (metricName != null)
        {
            Metrics.remove(metricName);
        }
    }

    public static void init()
    {
        // noop
    }
}
