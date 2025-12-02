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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;

import com.netflix.cassandra.metrics.ObjectStoreMetrics;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import software.amazon.awssdk.regions.Region;

/**
 * Interface for asynchronous object store operations with region-specific client management.
 *
 * This interface provides a generic abstraction for object storage operations such as reading objects,
 * listing keys, and retrieving metadata. While the current implementation is backed by AWS S3
 * (via {@link AwsObjectStoreAccess}), the interface is designed to support other object store
 * backends (e.g., Google Cloud Storage, Azure Blob Storage, MinIO, etc.).
 *
 * The interface maintains region-specific client instances and metrics for performance tracking.
 */
public interface ObjectStoreAccess
{

    /** Cache of region-specific client instances */
    Map<Region, ObjectStoreAccess> regionToClientMap = new ConcurrentHashMap<>();
    
    /** Global object store metrics instance */
    ObjectStoreMetrics GLOBAL_OBJECT_STORE_METRICS = new ObjectStoreMetrics();

    /**
     * Retrieves or creates an ObjectStoreAccess instance for the specified region with default retry settings.
     * Currently returns an AWS S3-backed implementation.
     *
     * @param region the region to get the client for (AWS region for S3 implementation)
     * @return ObjectStoreAccess instance for the region
     */
    static ObjectStoreAccess get(Region region) {
        return regionToClientMap.computeIfAbsent(
            region,
            __ -> AwsObjectStoreAccess.build(region, ObjectStoreConfiguration.defaultRetryConfig)
        );
    }

    /**
     * Retrieves or creates an ObjectStoreAccess instance for the specified region with provided retry settings.
     * Currently returns an AWS S3-backed implementation.
     *
     * @param region the region to get the client for (AWS region for S3 implementation)
     * @param objectStoreAccessConfiguration the retry configuration to use
     * @return ObjectStoreAccess instance for the region
     */
    static ObjectStoreAccess get(Region region, ObjectStoreConfiguration objectStoreAccessConfiguration) {
        return regionToClientMap.computeIfAbsent(
            region,
            __ -> AwsObjectStoreAccess.build(region, objectStoreAccessConfiguration)
        );
    }

    /**
     * Retrieves the global object store metrics instance.
     *
     * @return Global ObjectStoreMetrics instance
     */
    static ObjectStoreMetrics getMetrics() {
        return GLOBAL_OBJECT_STORE_METRICS;
    }

    /**
     * Sets a custom ObjectStoreAccess instance for the specified region for
     * testing or overriding the default client.
     *
     * @param region the region to associate with the client
     * @param objectStoreAccess the ObjectStoreAccess instance to use
     * @return the provided ObjectStoreAccess instance
     */
    @VisibleForTesting
    static ObjectStoreAccess set(Region region, ObjectStoreAccess objectStoreAccess) {
        regionToClientMap.put(region, objectStoreAccess);
        return objectStoreAccess;
    }

    /**
     * Downloads an object directly to a file.
     *
     * @param bucket the bucket name (e.g., S3 bucket)
     * @param key the object key
     * @param path the local file path to write to
     * @return AsyncPromise that completes when the download finishes
     */
    AsyncPromise<Void> getObjectAsFile(String bucket, String key, Path path);

    /**
     * Retrieves a specific byte range from an object into a reusable ByteBuffer to avoid allocations.
     *
     * @param bucket the bucket name (e.g., S3 bucket)
     * @param key the object key
     * @param from the starting byte position (inclusive)
     * @param to the ending byte position (inclusive)
     * @param buffer the ByteBuffer to reuse for the response data
     * @return AsyncPromise that completes when the operation finishes
     */
    AsyncPromise<Void> getObjectRangeIntoBuffer(String bucket, String key, long from, long to, ByteBuffer buffer);

    /**
     * Lists all object keys in a bucket with the specified prefix.
     *
     * @param bucket the bucket name (e.g., S3 bucket)
     * @param prefix the key prefix to filter by
     * @return AsyncPromise containing a list of matching object keys
     */
    AsyncPromise<List<String>> getObjectKeys(String bucket, String prefix);

    /**
     * Retrieves the size of an object in bytes.
     *
     * @param bucket the bucket name (e.g., S3 bucket)
     * @param key the object key
     * @return AsyncPromise containing the object size in bytes
     */
    AsyncPromise<Long> getObjectSize(String bucket, String key);
}
