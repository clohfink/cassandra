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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.MBeanWrapper;

public class BackupService implements BackupServiceMBean
{
    private static final Logger logger = LoggerFactory.getLogger(BackupService.class);

    public static final String MBEAN_NAME = "com.netflix.cassandra.backups:type=BackupService";

    public static final BackupService instance = new BackupService();

    private volatile RateLimiter backupTimestampLimiter;

    private BackupService()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME, MBeanWrapper.OnException.LOG);
    }

    /**
     * Blocks until a permit is available if a rate limit is configured;
     * no-op when no limit is set (the default).
     */
    public void acquireBackupTimestampPermit()
    {
        RateLimiter rl = backupTimestampLimiter;
        if (rl != null)
            rl.acquire();
    }

    @Override
    public double getBackupTimestampRateLimit()
    {
        RateLimiter rl = backupTimestampLimiter;
        return rl == null ? 0.0 : rl.getRate();
    }

    /**
     * To change at runtime: nodetool sjk mx
     *   -b com.netflix.cassandra.backups:type=BackupService
     *   -mc -op setBackupTimestampRateLimit
     *   -a 100
     * @param permitsPerSecond
     */
    @Override
    public synchronized void setBackupTimestampRateLimit(double permitsPerSecond)
    {
        if (permitsPerSecond <= 0)
        {
            backupTimestampLimiter = null;
            logger.info("Disabled backup timestamp rate limit");
            return;
        }
        RateLimiter rl = backupTimestampLimiter;
        if (rl == null)
            backupTimestampLimiter = RateLimiter.create(permitsPerSecond);
        else
            rl.setRate(permitsPerSecond);
        logger.info("Set backup timestamp rate limit to {} permits/sec", permitsPerSecond);
    }

    @Override
    public boolean isBackupManifestEnabled()
    {
        return DatabaseDescriptor.isBackupManifestEnabled();
    }

    @Override
    public void setBackupManifestEnabled(boolean enabled)
    {
        DatabaseDescriptor.setBackupManifestEnabled(enabled);
        logger.info("{} Netflix backup manifest", enabled ? "Enabled" : "Disabled");
    }
}
