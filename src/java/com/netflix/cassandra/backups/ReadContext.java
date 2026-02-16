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

package com.netflix.cassandra.backups;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.metrics.TableMetrics;

/**
 * Context object for tracking per-read metrics across multiple threads.
 * This class is designed to be passed through call stacks and shared across
 * threads that participate in a single read operation.
 *
 * All metric recording methods are thread-safe using atomic operations.
 */
public class ReadContext
{
    private final AtomicInteger totalChunksRead = new AtomicInteger();
    private final AtomicInteger prefetchesInitiated = new AtomicInteger();
    private final AtomicInteger cacheHits = new AtomicInteger();
    private final AtomicLong bytesFetched = new AtomicLong();

    /**
     * Record that a chunk was read (either from cache or S3).
     */
    public void recordChunkRead()
    {
        totalChunksRead.incrementAndGet();
    }

    /**
     * Record that a prefetch operation was initiated for a partition.
     */
    public void recordPrefetchInitiated()
    {
        prefetchesInitiated.incrementAndGet();
    }

    /**
     * Record that a chunk was served from the shared chunk cache.
     */
    public void recordCacheHit()
    {
        cacheHits.incrementAndGet();
    }

    /**
     * Record the number of compressed bytes fetched from S3.
     *
     * @param bytes the number of bytes fetched
     */
    public void recordBytesFetched(long bytes)
    {
        bytesFetched.addAndGet(bytes);
    }

    /**
     * Report all accumulated metrics to the provided TableMetrics instance.
     * This should be called once at the end of a read operation.
     *
     * @param metrics the TableMetrics instance to report to
     */
    public void reportToTableMetrics(TableMetrics metrics)
    {
        if (metrics == null)
            return;

        int chunks = totalChunksRead.get();
        int prefetches = prefetchesInitiated.get();
        int hits = cacheHits.get();
        long bytes = bytesFetched.get();

        if (chunks > 0)
            metrics.objectStoreChunksPerRead.update(chunks);
        if (prefetches > 0)
            metrics.objectStorePrefetchesPerRead.update(prefetches);
        if (hits > 0)
            metrics.objectStoreCacheHitsPerRead.update(hits);
        if (bytes > 0)
            metrics.objectStoreBytesPerRead.update(bytes);
    }

    @Override
    public String toString()
    {
        return String.format("ReadContext{chunks=%d, prefetches=%d, cacheHits=%d, s3Bytes=%d}",
                             totalChunksRead.get(), prefetchesInitiated.get(),
                             cacheHits.get(), bytesFetched.get());
    }
}