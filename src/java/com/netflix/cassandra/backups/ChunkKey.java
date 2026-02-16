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

import java.util.Objects;

public class ChunkKey
{
    private final String bucket;
    private final String key;
    private final long chunkIndex;
    private final int hashCode;

    public ChunkKey(String bucket, String key, long chunkIndex)
    {
        this.bucket = bucket;
        this.key = key;
        this.chunkIndex = chunkIndex;
        this.hashCode = computeHashCode();
    }

    private int computeHashCode()
    {
        int result = bucket.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + Long.hashCode(chunkIndex);
        return result;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        ChunkKey chunkKey = (ChunkKey) o;
        return chunkIndex == chunkKey.chunkIndex && Objects.equals(bucket, chunkKey.bucket) && Objects.equals(key, chunkKey.key);
    }

    @Override
    public String toString()
    {
        return String.format("ChunkKey{bucket='%s', key='%s', chunkIndex=%d}", bucket, key, chunkIndex);
    }
}
