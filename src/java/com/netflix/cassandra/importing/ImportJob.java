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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.steps.DoneStep;
import com.netflix.cassandra.importing.steps.DownloadUnzipStep;
import com.netflix.cassandra.metrics.ImportJobMetrics;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Orchestrates the Netflix Cassandra import process through a state machine of sequential steps.
 * Manages both synchronous and asynchronous operations with automatic rescheduling and error handling.
 *
 * <h3>Core State Management</h3>
 * The job uses a two-phase execution model for each step:
 * <ul>
 * <li><b>init()</b> - Called once per step when transitioning to it via checkState(). Initial step is NOT initialized in constructor. Performs setup and starts operations</li>
 * <li><b>checkComplete()</b> - Called repeatedly to monitor progress and determine transitions</li>
 * </ul>
 *
 * <h3>Scheduling and Rescheduling</h3>
 * <b>Synchronous Steps:</b> Complete immediately, return next step from checkComplete()<br>
 * <b>Asynchronous Steps:</b> Return current step from checkComplete(), get rescheduled based on getNextCheckDelayMs()
 * <p>
 * The {@code checkState()} method:
 * <ol>
 * <li>Calls {@code checkComplete()} to get next step or self</li>
 * <li>If transitioning to a new step, calls {@code init()} on it</li>
 * <li>Updates current step and status if transitioning</li>
 * <li>Schedules next execution if step returns delay &lt; Long.MAX_VALUE</li>
 * </ol>
 *
 * <h3>Error Handling and Cleanup</h3>
 * All steps support cancellation via {@code cleanup()} method to stop async operations and release resources.
 * Timeouts, disk space failures, and exceptions automatically transition to ERROR state.
 */
public class ImportJob
{
    public static final ScheduledExecutorPlus executor = executorFactory().scheduled(false, "S3ImportTasks");
    // Constants for timeout and delay values
    static final long DEFAULT_TIMEOUT_MS = TimeUnit.HOURS.toMillis(1);
    private static final Logger logger = LoggerFactory.getLogger(ImportJob.class);
    public final String targetKeyspace;
    public final String targetTable;
    public final Date createdAt;
    public final AtomicReference<ImportStatus> status = new AtomicReference<>();
    public final UUID id;
    public String errorMessage;
    public ImportStatus failedAtStep;
    protected File stagingDirectory;
    private ImportStep currentStep;
    private volatile ScheduledFuture<?> nextCheckFuture;
    private long currentStepStartTime;
    private final ConcurrentMap<ImportStatus, Long> stepStartTimes = new ConcurrentHashMap<>();

    @VisibleForTesting
    static Clock CLOCK = new Clock.Default();

    /**
     * Creates an ImportJob with the default system clock.
     */
    protected ImportJob(UUID id, String targetKeyspace, String targetTable)
    {
        this(id, targetKeyspace, targetTable, null);
    }

    ImportJob(UUID id, String targetKeyspace, String targetTable, ImportStep step)
    {
        this.id = id;
        this.targetKeyspace = targetKeyspace;
        this.targetTable = targetTable;
        this.createdAt = new Date();
        if (step == null)
        {
            step = createInitialStep();
        }
        this.currentStep = step;
        this.status.set(step.getStatus());
        this.currentStepStartTime = CLOCK.currentTimeMillis();
        stepStartTimes.put(step.getStatus(), CLOCK.currentTimeMillis());
        try
        {
            this.currentStep.init();
        }
        catch (Exception e)
        {
            setError(e.getMessage());
        }
        ImportJobMetrics.instance.jobStarted(this);
    }

    public static List<RemoteImportRecord> queryRemoteImport(UUID id, String targetKeyspace, String targetTable)
    {
        String query = "SELECT id, target_keyspace, target_table, state, source, source_type, start_token, end_token, dc_filter, size " +
                       "FROM system_distributed.remote_import WHERE id = ? AND target_keyspace = ? AND target_table = ?";

        UntypedResultSet results;
        try
        {
            // Try first with QUORUM consistency level for strong consistency
            results = QueryProcessor.execute(query, ConsistencyLevel.QUORUM, id, targetKeyspace, targetTable);
        }
        catch (Exception e)
        {
            logger.debug("Failed to execute remote_import query at QUORUM consistency level, falling back to ONE: {}", e.getMessage());
            ImportJobMetrics.instance.consistencyLevelFallback();
            // Fallback to ONE consistency level if QUORUM fails
            results = QueryProcessor.execute(query, ConsistencyLevel.ONE, id, targetKeyspace, targetTable);
        }

        List<RemoteImportRecord> records = new ArrayList<>();
        for (UntypedResultSet.Row row : results)
        {
            records.add(new RemoteImportRecord(
                row.getUUID("id"),
                row.getString("target_keyspace"),
                row.getString("target_table"),
                row.has("state") ? row.getString("state") : null,
                row.getString("source"),
                row.has("source_type") ? row.getString("source_type") : null,
                row.has("start_token") ? row.getString("start_token") : null,
                row.has("end_token") ? row.getString("end_token") : null,
                row.has("dc_filter") ? row.getString("dc_filter") : null,
                row.has("size") ? row.getLong("size") : null
            ));
        }

        return records;
    }

    protected final ImportStep createInitialStep()
    {
        ImportStep step = doCreateInitialStep();
        Preconditions.checkState(step != null, "createInitialStep implementation must return a non-null step");
        return step;
    }

    protected ImportStep doCreateInitialStep()
    {
        throw new NotImplementedException("Either step must be specified or createInitialStep must be overridden");
    }

    public Map<String, String> getStatusMap()
    {
        if (status.get() == ImportStatus.ERROR)
        {
            Map<String, String> errorStatus = new HashMap<>();
            errorStatus.put("step", "Error");
            if (failedAtStep != null)
            {
                errorStatus.put("failedStep", failedAtStep.name().toLowerCase());
            }
            errorStatus.put("description", errorMessage != null ? errorMessage : "An error occurred during the import process");
            errorStatus.put("progress", "0.0"); // Progress is 0 because the job failed and no further progress will be made
            return errorStatus;
        }
        else if (status.get() == ImportStatus.CANCELLED)
        {
            Map<String, String> cancelledStatus = new HashMap<>();
            cancelledStatus.put("step", "Cancelled");
            if (failedAtStep != null)
            {
                cancelledStatus.put("failedStep", failedAtStep.name().toLowerCase());
            }
            cancelledStatus.put("description", errorMessage != null ? errorMessage : "The import job has been cancelled");
            cancelledStatus.put("progress", "0.0"); // Progress is 0 because the job was cancelled and no further progress will be made
            return cancelledStatus;
        }
        return currentStep.toStatusMap();
    }

    public synchronized void cancel(String reason)
    {
        // Cancel any scheduled next step execution
        if (nextCheckFuture != null && !nextCheckFuture.isDone())
        {
            nextCheckFuture.cancel(false);
            nextCheckFuture = null;
        }

        // Cleanup current step's async operations
        if (currentStep != null)
        {
            currentStep.cleanup();
        }

        this.failedAtStep = currentStep != null ? currentStep.getStatus() : null;
        status.set(ImportStatus.CANCELLED);
        this.errorMessage = reason;
        this.currentStep = new DoneStep(targetKeyspace, targetTable);

        logger.info("Import job {} for {}.{} has been cancelled in {} state: {}", id, targetKeyspace, targetTable, failedAtStep != null ? failedAtStep.name().toLowerCase() : "unknown", reason);

        // Track job cancellation in metrics
        ImportJobMetrics.instance.jobCancelled(this);
    }

    public synchronized void reset()
    {
        // Cancel any scheduled next step execution
        if (nextCheckFuture != null && !nextCheckFuture.isDone())
        {
            nextCheckFuture.cancel(false);
            nextCheckFuture = null;
        }

        // Cleanup current step's async operations
        if (currentStep != null)
        {
            currentStep.cleanup();
        }

        // Clean up staging directory if it exists
        cleanupStagingDirectory();

        // Reset status and restart from initial step
        status.set(ImportStatus.VALIDATING);
        currentStep = createInitialStep();
        currentStepStartTime = CLOCK.currentTimeMillis();
        errorMessage = null;
        failedAtStep = null;

        logger.info("Import job {}.{} has been reset and will restart from initial step", targetKeyspace, targetTable);
    }

    protected void cleanupStagingDirectory()
    {
        if (stagingDirectory != null && stagingDirectory.exists())
        {
            try
            {
                stagingDirectory.deleteRecursive();
                logger.info("Cleaned up staging directory: {}", stagingDirectory.absolutePath());
            }
            catch (Exception e)
            {
                ImportJobMetrics.instance.cleanupError();
                logger.warn("Failed to cleanup staging directory {}: {}", stagingDirectory.absolutePath(), e.getMessage());
            }
            finally
            {
                stagingDirectory = null;
            }
        }
    }

    protected void setError(String message)
    {
        this.errorMessage = message;
        this.failedAtStep = currentStep != null ? currentStep.getStatus() : null;
        this.currentStep = new DoneStep(targetKeyspace, targetTable);
        this.status.set(ImportStatus.ERROR);
        logger.error("Failed in {} state for import job {} ({}.{}): {}", failedAtStep != null ? failedAtStep.name().toLowerCase() : "unknown", id, targetKeyspace, targetTable, message, new Throwable(message));

        // Track job failure in metrics
        ImportJobMetrics.instance.jobFailed(this);
    }

    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownAndWait(timeout, unit, executor, DownloadUnzipStep.retryExecutor);
    }

    public synchronized void checkState()
    {
        try
        {
            ImportStep nextStep = currentStep.checkComplete();
            if (nextStep != null && nextStep != currentStep)
            {
                // Stop timing for previous step
                ImportStatus currentStatus = currentStep.getStatus();
                Long stepStartTime = stepStartTimes.get(currentStatus);
                if (stepStartTime != null)
                {
                    long stepDuration = CLOCK.currentTimeMillis() - stepStartTime;
                    logger.debug("Step transition: {} completed in {}ms, transitioning to {}",
                                currentStatus, stepDuration, nextStep.getStatus());
                    ImportJobMetrics.instance.recordStepDuration(currentStatus, stepDuration);
                    stepStartTimes.remove(currentStatus);
                }

                // Initialize the new step before transitioning
                nextStep.init();

                // incase an error occurred, we don't want to change the status
                if (status.get() != ImportStatus.ERROR && status.compareAndSet(currentStatus, nextStep.getStatus()))
                {
                    ImportStatus nextStatus = nextStep.getStatus();

                    // Start timing for new step
                    long newStepStart = CLOCK.currentTimeMillis();
                    stepStartTimes.put(nextStatus, newStepStart);
                    logger.debug("Step transition: {} started at {}", nextStatus, newStepStart);

                    currentStep = nextStep;
                    currentStepStartTime = newStepStart;
                    // Track status change in metrics
                    ImportJobMetrics.instance.statusChanged(nextStatus);
                }
            }

            // Check timeout using current step's timeout and our tracked start time
            long elapsed = CLOCK.currentTimeMillis() - currentStepStartTime;
            long timeout = currentStep.timeoutMillis();
            if (elapsed > timeout)
            {
                ImportJobMetrics.instance.timeoutError();
                setError("Import job timed out after " + timeout + " ms in state " + currentStep.getStatus());
            }
        }
        catch (Throwable e)
        {
            String state = currentStep != null ? currentStep.getStatus().toString() : "UNKNOWN";
            logger.error("Error during import job {}.{} in state {}: {}", targetKeyspace, targetTable, state, e.getMessage(), e);
            setError(e.getMessage());
        }
        finally
        {
            // Schedule next check if needed
            if (currentStep != null)
            {
                long next = currentStep.getNextCheckDelayMs();
                if (next != Long.MAX_VALUE && next > 1)
                {
                    nextCheckFuture = executor.schedule(this::checkState, next, TimeUnit.MILLISECONDS);
                }
            }
        }
    }
}