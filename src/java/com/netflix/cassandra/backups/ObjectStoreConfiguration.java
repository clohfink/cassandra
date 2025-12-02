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

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for retry settings used in object store operations.
 *
 * This configuration is used by {@link ObjectStoreAccess} implementations to control
 * retry behavior. While currently used primarily with AWS S3 CRT client
 * (via {@link AwsObjectStoreAccess}), it is designed to be generic and applicable
 * to other object store backends.
 */
public class ObjectStoreConfiguration
{
    private static final int defaultMaxRetries = 2; // 2 retries

    /**
     * Maximum number of retries for an API call.
     */
    public int maxRetries;

    /**
     * Default retry configuration with {@link #defaultMaxRetries} retries.
     */
    public static final ObjectStoreConfiguration defaultRetryConfig = ObjectStoreConfiguration.builder().build();

    private ObjectStoreConfiguration() {
        // Private constructor to enforce use of Builder
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        int maxRetries = defaultMaxRetries;

        public Builder withMaxRetries(int retries) {
            this.maxRetries = retries;
            return this;
        }

        public ObjectStoreConfiguration build() {
            ObjectStoreConfiguration config = new ObjectStoreConfiguration();
            config.maxRetries = this.maxRetries;
            return config;
        }
    }

    /**
     * Creates an ObjectStoreConfiguration from a key-value string 'a=val1,b=val2,...'.
     *
     * @param configString comma-separated key-value pairs (e.g., "maxRetries=5")
     * @return ObjectStoreConfiguration instance with parsed settings
     */
    public static ObjectStoreConfiguration fromKeyValueString(String configString) {
        Builder builder = ObjectStoreConfiguration.builder();
        Map<String, String> map = new HashMap<>();
        String[] pairs = configString.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0].trim(), kv[1].trim());
            }
        }

        if (map.containsKey("maxRetries"))
            builder.withMaxRetries(Integer.parseInt(map.get("maxRetries")));

        return builder.build();
    }
}
