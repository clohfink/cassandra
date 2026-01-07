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

package com.netflix.cassandra.importing;


import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.disk.usage.DiskUsageMonitor;

/**
 * Interface for all import steps in the S3 import process.
 * Each step represents a distinct phase in the import workflow, such as validation, filtering,
 * downloading, and importing SSTables from S3.
 * <p>
 * The import process follows a state machine pattern where each step:
 * 1. Is initialized via init() when it becomes the current step
 * 2. Has its state checked repeatedly via checkComplete()
 * 3. Returns the next step to transition to, or itself to continue, or null if the process should stop
 * <p>
 * Steps are executed sequentially by the ImportJob, which manages the overall import process,
 * calls init() on new steps during transitions, and maintains the current state.
 */
@NotThreadSafe
public interface ImportStep
{
    Logger logger = LoggerFactory.getLogger(ImportStep.class);
    long DEFAULT_CHECK_DELAY_MS = 1000;

    /**
     * Returns the delay in milliseconds before the next state check should be performed.
     * This allows steps to control the frequency of their execution.
     *
     * @return delay in milliseconds before next check
     */
    default long getNextCheckDelayMs()
    {
        return DEFAULT_CHECK_DELAY_MS; // Default delay, can be overridden by specific steps
    }

    /**
     * Returns the timeout in milliseconds for this step.
     * ImportJob uses this value along with its tracked start time to determine if the step has timed out.
     *
     * @return timeout in milliseconds
     */
    default long timeoutMillis()
    {
        return ImportJob.DEFAULT_TIMEOUT_MS; // Default timeout of 1 hour
    }

    /**
     * Initializes this import step. This method is called once by ImportJob when transitioning to a new step.
     * <p>
     * <b>Important:</b> The initial step is NOT automatically initialized in the constructor. It will be initialized
     * on the first call to checkState() if it transitions to a new step, or remains uninitialized if it immediately
     * returns itself from checkComplete().
     * <p>
     * For all subsequent steps: called in checkState() when a new step is returned by checkComplete().
     * <p>
     * Allows steps to perform any necessary setup or initialization before checkComplete() is called.
     */
    default void init()
    {
        // Default implementation does nothing, can be overridden by specific steps
    }

    /**
     * Cleanup method called when the step is being cancelled or reset.
     * Implementations should cancel any ongoing async operations, close resources,
     * and perform any necessary cleanup to prevent resource leaks.
     */
    default void cleanup()
    {
        // Default implementation does nothing, can be overridden by specific steps
    }

    /**
     * Checks the current state of the import process and determines the next step to execute.
     * This method is called repeatedly by the ImportJob until it returns an exception or a step with
     * getNextCheckDelay of Long.MAX
     *
     * @return the next ImportStep to execute, or null if the process should stop
     * @throws Exception if an error occurs during state checking
     */
    ImportStep checkComplete() throws Throwable;

    /**
     * Returns the current status of this import step.
     * This status is used by the ImportJob to track the overall progress of the import process.
     *
     * @return the current ImportStatus of this step
     */
    ImportStatus getStatus();

    /**
     * Returns the progress of this import step as a value between 0.0 and 1.0.
     * 
     * @return progress value between 0.0 (not started) and 1.0 (completed)
     */
    double getProgress();

    default Map<String, String> baseStatusMap()
    {
        Map<String, String> status = new HashMap<>();
        status.put("step", getStatus().name());
        status.put("progress", String.format("%.3f", getProgress()));
        return status;
    }

    /**
     * Returns a map of human-readable status information for this import step.
     * This provides detailed information about the current state of the step that can be
     * used for monitoring and debugging purposes.
     *
     * @return a map containing human-readable status information
     */
    Map<String, String> toStatusMap();

    static void checkImportDiskSpace(long additionalBytes) throws ImportDiskSpaceException
    {
        double currentUsageRatio = DiskUsageMonitor.instance.getDiskUsage();
        double currentUsagePercentage = currentUsageRatio * 100.0;

        int maxDiskPercentage = DatabaseDescriptor.getImportMaxDiskPercentage();

        if (currentUsagePercentage > maxDiskPercentage)
        {
            throw new ImportDiskSpaceException(String.format(
            "Current disk usage exceeds import threshold: %.1f%% > %d%%",
            currentUsagePercentage, maxDiskPercentage));
        }

        if (additionalBytes > 0)
        {
            long totalDiskSpace = DiskUsageMonitor.totalDiskSpace();
            double projectedUsageRatio = currentUsageRatio + ((double) additionalBytes / totalDiskSpace);
            double projectedUsagePercentage = projectedUsageRatio * 100.0;

            if (projectedUsagePercentage > maxDiskPercentage)
            {
                throw new ImportDiskSpaceException(String.format(
                "Import would exceed disk usage threshold. " +
                "Current: %.1f%%, projected: %.1f%%, max allowed: %d%% " +
                "(additional %d bytes needed)",
                currentUsagePercentage, projectedUsagePercentage, maxDiskPercentage, additionalBytes));
            }

            logger.debug("Import disk space check passed - current: {}%, projected: {}%, max: {}%",
                         currentUsagePercentage, projectedUsagePercentage, maxDiskPercentage);
        }
        else
        {
            logger.debug("Import disk space check passed - current usage: {}%, max allowed: {}%",
                         currentUsagePercentage, maxDiskPercentage);
        }
    }

    class ImportDiskSpaceException extends Exception
    {
        public ImportDiskSpaceException(String message)
        {
            super(message);
        }
    }
}