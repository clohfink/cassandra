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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.netflix.cassandra.backups.BackupChunkReader;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class ColdTierMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(ColdTierMetrics.class);
    public static final String TYPE_NAME = "ColdTier";
    public static final Histogram chunksPerRead = Metrics.histogram(
    DefaultNameFactory.createMetricName(TYPE_NAME, "ChunksPerRead", null), false
    );

    // Shared Chunk Cache metrics
    public static final Gauge<Long> sharedChunkCacheSize = Metrics.register(
    DefaultNameFactory.createMetricName(TYPE_NAME, "SharedChunkCacheSize", null),
    BackupChunkReader::getSharedChunkCacheSize
    );

    public static final Gauge<Long> sharedChunkCacheCapacity = Metrics.register(
    DefaultNameFactory.createMetricName(TYPE_NAME, "SharedChunkCacheCapacity", null),
    BackupChunkReader::getSharedChunkCacheCapacity
    );

    public static final Gauge<Double> sharedChunkCacheUtilization = Metrics.register(
    DefaultNameFactory.createMetricName(TYPE_NAME, "SharedChunkCacheUtilization", null),
    BackupChunkReader::getSharedChunkCacheUtilization
    );

    public static final Gauge<Long> sharedChunkCacheOldestEntryAgeMillis = Metrics.register(
    DefaultNameFactory.createMetricName(TYPE_NAME, "SharedChunkCacheOldestEntryAgeMillis", null),
    BackupChunkReader::getSharedChunkCacheOldestEntryAgeMillis
    );

    public static final Gauge<Long> sharedChunkCacheNewestEntryAgeMillis = Metrics.register(
    DefaultNameFactory.createMetricName(TYPE_NAME, "SharedChunkCacheNewestEntryAgeMillis", null),
    BackupChunkReader::getSharedChunkCacheNewestEntryAgeMillis
    );

    public static final Gauge<Long> sharedChunkCacheAverageEntryAgeMillis = Metrics.register(
    DefaultNameFactory.createMetricName(TYPE_NAME, "SharedChunkCacheAverageEntryAgeMillis", null),
    BackupChunkReader::getSharedChunkCacheAverageEntryAgeMillis
    );

    public static final Histogram backupMemtableInitTimeMs = Metrics.histogram(
    DefaultNameFactory.createMetricName(TYPE_NAME, "BackupMemtableInitTimeMs", null), false
    );
}