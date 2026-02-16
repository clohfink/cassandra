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

import java.nio.ByteBuffer;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Wrapper for cached ByteBuffer that can be marked as invalidated when evicted from cache.
 * This prevents data races where a buffer is reused while another thread is still reading from it.
 */
class CachedChunk
{
    private final ByteBuffer buffer;
    private volatile boolean valid = true;
    private final long creationTimeNanos;

    CachedChunk(ByteBuffer buffer)
    {
        this.buffer = buffer;
        this.creationTimeNanos = nanoTime();
    }

    ByteBuffer getBuffer()
    {
        return buffer;
    }

    boolean isValid()
    {
        return valid;
    }

    void invalidate()
    {
        valid = false;
    }

    /**
     * Get the age of this cached chunk in milliseconds.
     */
    long getAgeMillis()
    {
        return (nanoTime() - creationTimeNanos) / 1_000_000;
    }
}
