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

import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import software.amazon.awssdk.regions.Region;

public class BackupContext
{
    private static final Set<String> VALID_ENVIRONMENTS = Set.of("test", "prod");
    private final String env;
    private final String app;
    private final String region;
    private final String token;
    private final String bucketOverride;
    private final String prefixOverride;

    public BackupContext(String env, String region, String app, String token)
    {
        this(env, region, app, token, null, null);
    }

    public BackupContext(String env, String region, String app, String token, String bucketOverride, String prefixOverride)
    {
        this.env = env;
        this.region = region;
        this.app = app;
        this.token = token;
        this.bucketOverride = bucketOverride;
        this.prefixOverride = prefixOverride;
    }

    public String env()
    {
        return env;
    }

    public String region()
    {
        return region;
    }

    public String app()
    {
        return app;
    }

    public String token()
    {
        return token;
    }

    public String bucket()
    {
        if (bucketOverride != null)
            return bucketOverride;
        return region.replaceAll("-", "") + "-cass-" + env + "-1";
    }

    public String prefix()
    {
        if (prefixOverride != null)
            return prefixOverride;
        return env + "_backup" + '/' + String.format("%d_%s", app.hashCode() % 10000, app);
    }

    public String metafilePrefix()
    {
        return prefix() + '/' + token + "/META_V2/";
    }

    public String sstV2Prefix()
    {
        return prefix() + '/' + token + "/SST_V2/";
    }

    public boolean isValid()
    {
        return StringUtils.isNotBlank(env)
               && StringUtils.isNotBlank(region)
               && StringUtils.isNotBlank(app)
               && StringUtils.isNotBlank(token)
               && Region.regions().stream().map(Region::id).anyMatch(regionId -> regionId.equals(region))
               && VALID_ENVIRONMENTS.contains(env);
    }
}