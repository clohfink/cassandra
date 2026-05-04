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

public interface BackupServiceMBean
{
    /**
     * Returns the configured rate limit (permits/sec) applied to
     * {@code SystemKeyspace.getOrAssignBackupTimestamp()}, or {@code 0}
     * if rate limiting is disabled.
     */
    double getBackupTimestampRateLimit();

    /**
     * Sets the rate limit (permits/sec) applied to
     * {@code SystemKeyspace.getOrAssignBackupTimestamp()}. A value of
     * {@code 0} or negative disables rate limiting.
     */
    void setBackupTimestampRateLimit(double permitsPerSecond);

    /**
     * Returns whether the Netflix backup manifest writer is enabled
     * (cassandra.yaml: {@code netflix_backup_manifest_enabled}).
     */
    boolean isBackupManifestEnabled();

    /**
     * Enables or disables the Netflix backup manifest writer at runtime.
     */
    void setBackupManifestEnabled(boolean enabled);
}
