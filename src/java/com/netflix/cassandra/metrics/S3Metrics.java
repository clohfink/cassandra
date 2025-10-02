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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.metrics.LatencyMetrics;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class S3Metrics extends LatencyMetrics
{
    public final Meter successes;
    public final Meter failures;
    public final Meter accessDenied;

    public final Timer objectFetchLatency;
    public final Histogram objectFetchBytes;

    public final Timer rangeReadFetchLatency;
    public final Histogram rangeReadFetchBytes;

    public final Timer prefixFetchLatency;

    public final Timer listObjectLatency;

    public final Timer headObjectLatency;

    public S3Metrics()
    {
        super("S3", null);
        successes = Metrics.meter(factory.createMetricName("Successes"));
        failures = Metrics.meter(factory.createMetricName("Failures"));
        accessDenied = Metrics.meter(factory.createMetricName("AccessDenied"));
        objectFetchLatency = Metrics.timer(factory.createMetricName("ObjectFetchLatency"));
        objectFetchBytes = Metrics.histogram(factory.createMetricName("ObjectFetchBytes"), false);
        rangeReadFetchLatency = Metrics.timer(factory.createMetricName("RangeReadFetchLatency"));
        rangeReadFetchBytes = Metrics.histogram(factory.createMetricName("RangeReadFetchBytes"), false);
        prefixFetchLatency = Metrics.timer(factory.createMetricName("PrefixFetchLatency"));
        listObjectLatency = Metrics.timer(factory.createMetricName("ListObjectLatency"));
        headObjectLatency = Metrics.timer(factory.createMetricName("HeadObjectLatency"));
    }

    public void release()
    {
        super.release();
        Metrics.remove(factory.createMetricName("Successes"));
        Metrics.remove(factory.createMetricName("Failures"));
        Metrics.remove(factory.createMetricName("AccessDenied"));
        Metrics.remove(factory.createMetricName("ObjectFetchLatency"));
        Metrics.remove(factory.createMetricName("ObjectFetchBytes"));
        Metrics.remove(factory.createMetricName("RangeReadFetchLatency"));
        Metrics.remove(factory.createMetricName("RangeReadFetchBytes"));
        Metrics.remove(factory.createMetricName("PrefixFetchLatency"));
        Metrics.remove(factory.createMetricName("ListObjectLatency"));
        Metrics.remove(factory.createMetricName("HeadObjectLatency"));
    }
}
