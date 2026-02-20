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

package com.netflix.cassandra.importing.steps;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.ImportJobManager;
import com.netflix.cassandra.importing.ImportStatus;
import com.netflix.cassandra.importing.ImportStep;
import com.netflix.cassandra.metrics.ImportJobMetrics;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;

/**
 * Performs cleanup operations to remove non-owned data after importing SSTables.
 * <p>
 * <b>Import Flow:</b> VALIDATING → FILTERING → DOWNLOADING → STAGED → IMPORTING → [<b>TRIMMING</b>] → DONE
 * <p>
 * <b>Input:</b> Imported SSTables from ImportingStep<br>
 * <b>Output:</b> Cleaned up table ready for completion in DoneStep<br>
 * <b>Execution Type:</b> Synchronous cleanup operation
 * <p>
 * <b>init():</b> Performs ColumnFamilyStore.forceCleanup(2) to remove data that doesn't belong
 * to this node's token ranges, using a parallelism level of 2<br>
 * <b>checkComplete():</b> Records job completion metrics and always transitions immediately
 * after cleanup completes successfully
 */
public class TrimStep implements ImportStep
{
    private static final Logger logger = LoggerFactory.getLogger(TrimStep.class);
    private volatile boolean trimCompleted = false;
    private final String targetKeyspace;
    private final String targetTable;
    private final UUID jobId;

    public TrimStep(UUID jobId, String targetKeyspace, String targetTable)
    {
        this.targetKeyspace = targetKeyspace;
        this.targetTable = targetTable;
        this.jobId = jobId;
    }

    @Override
    public void init()
    {
        int maxRetries = DatabaseDescriptor.getImportCleanupMaxRetries();
        long initialDelayMillis = DatabaseDescriptor.getImportCleanupRetryInitialDelayMillis();

        try (var timer = ImportJobMetrics.instance.startTrimTimer())
        {
            for (int attempt = 0; attempt <= maxRetries; attempt++)
            {
                try
                {
                    performCleanup();
                    trimCompleted = true;
                    return;
                }
                catch (ExecutionException | RuntimeException e)
                {
                    if (findCompactionInterruptedException(e) == null)
                        throw e;
                    if (attempt == maxRetries)
                    {
                        logger.error("Cleanup for {}.{} interrupted after {} retries, giving up",
                                     targetKeyspace, targetTable, maxRetries, e);
                        throw e;
                    }
                    long delayMillis = initialDelayMillis * (1L << attempt);
                    logger.warn("Cleanup for {}.{} was interrupted (attempt {}/{}), retrying in {}s",
                                targetKeyspace, targetTable, attempt + 1, maxRetries + 1,
                                TimeUnit.MILLISECONDS.toSeconds(delayMillis), e);
                    Uninterruptibles.sleepUninterruptibly(delayMillis, TimeUnit.MILLISECONDS);
                }
            }
        }
        catch (ExecutionException | InterruptedException e)
        {
            ImportJobMetrics.instance.trimStepError();
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    protected void performCleanup() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(targetKeyspace, targetTable);
        cfs.forceCleanup(2);
    }

    static Throwable findCompactionInterruptedException(Throwable t)
    {
        while (t != null)
        {
            if (t instanceof CompactionInterruptedException)
                return t;
            t = t.getCause();
        }
        return null;
    }

    @Override
    public ImportStep checkComplete() throws ExecutionException, InterruptedException
    {
        // Track job completion when trimming is complete
        ImportJobMetrics.instance.jobCompleted(ImportJobManager.getInstance().getJob(jobId));
        return new DoneStep(targetKeyspace, targetTable);
    }

    @Override
    public ImportStatus getStatus()
    {
        return ImportStatus.TRIMMING;
    }

    @Override
    public double getProgress()
    {
        return trimCompleted ? 1.0 : 0.0;
    }

    @Override
    public Map<String, String> toStatusMap()
    {
        Map<String, String> status = baseStatusMap();
        status.put("description", "Trim job is running cleanups");
        return status;
    }

    @Override
    public void cleanup()
    {
        // Cleanup operations cannot be easily cancelled once started,
        // but we can log the cleanup attempt for monitoring
        logger.info("Cleanup requested for trimming step in job {}.{}", targetKeyspace, targetTable);
    }
}
