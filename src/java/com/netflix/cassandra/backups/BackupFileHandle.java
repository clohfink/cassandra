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

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.utils.concurrent.Ref;

public class BackupFileHandle extends FileHandle
{
    private final String bucket;
    private final String key;
    private final long fileLength;
    private final ObjectStoreAccess access;

    public BackupFileHandle(ObjectStoreAccess access, String bucket, String key, CompressionMetadata compression, long fileLength)
    {
        super(new Cleanup(null, null, compression, null), null, null, compression, -1);
        this.bucket = bucket;
        this.key = key;
        this.fileLength = fileLength;
        this.access = access;
    }

    @Override
    public String path()
    {
        return key;
    }

    @Override
    public long dataLength()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public RebuffererFactory rebuffererFactory()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public FileHandle sharedCopy()
    {
        throw new RuntimeException("not implemented");
    }

    @Override
    public RandomAccessReader createReader()
    {
        return new BackupChunkReader(access,null, this.compressionMetadata().orElse(null), fileLength, bucket, key);
    }

    @Override
    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return new BackupChunkReader(access,null, this.compressionMetadata().orElse(null), fileLength, bucket, key);
    }

    @Override
    public FileDataInput createReader(long position)
    {
        BackupChunkReader reader = new BackupChunkReader(access,null, this.compressionMetadata().orElse(null), fileLength, bucket, key);
        reader.seek(position);
        return reader;
    }

    public String toString()
    {
        return String.format("BackupFileHandle[bucket=%s, key=%s, fileLength=%d]", bucket, key, fileLength);
    }
    @Override
    public void dropPageCache(long before)
    {
    }
}
