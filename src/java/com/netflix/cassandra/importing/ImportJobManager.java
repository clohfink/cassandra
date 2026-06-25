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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.steps.DownloadUnzipStep;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.MBeanWrapper;

public class ImportJobManager implements ImportJobManagerMBean
{
    private static final Logger logger = LoggerFactory.getLogger(ImportJobManager.class);

    public static final String MBEAN_NAME = "com.netflix.cassandra.importing:type=ImportJobManager";

    private static final ImportJobManager instance = new ImportJobManager();

    private final Map<UUID, ImportJob> jobs = new ConcurrentHashMap<>();
    private volatile ScheduledFuture<?> cleanupTask;

    private ImportJobManager()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME, MBeanWrapper.OnException.LOG);
        // Schedule cleanup task using configured values
        scheduleCleanupTask();
    }

    private synchronized void scheduleCleanupTask()
    {
        int initialDelaySeconds = DatabaseDescriptor.getImportCleanupInitialDelaySeconds();
        int periodSeconds = DatabaseDescriptor.getImportCleanupPeriodSeconds();
        cleanupTask = ScheduledExecutors.optionalTasks.scheduleWithFixedDelay(
            this::cleanupOrphanedJobs,
            initialDelaySeconds,
            periodSeconds,
            TimeUnit.SECONDS
        );
        logger.info("Scheduled import job cleanup task: initial delay {}s, period {}s",
                   initialDelaySeconds, periodSeconds);
    }

    private synchronized void rescheduleCleanupTask()
    {
        // Cancel existing task
        if (cleanupTask != null && !cleanupTask.isDone())
        {
            logger.info("Cancelling existing cleanup task");
            cleanupTask.cancel(false);
        }

        // Schedule new task with updated parameters
        scheduleCleanupTask();
    }
    
    public static ImportJobManager getInstance()
    {
        return instance;
    }
    
    public ImportJob getOrCreateJob(UUID snapshotId, String keyspace, String table)
    {
        return jobs.computeIfAbsent(snapshotId, id -> {
            try
            {
                List<RemoteImportRecord> results = ImportJob.queryRemoteImport(snapshotId, keyspace, table);

                if (!results.isEmpty())
                {
                    UrlImportJob urlJob = new UrlImportJob(snapshotId, keyspace, table);
                    try
                    {
                        urlJob.checkState();
                    }
                    catch (Exception checkStateException)
                    {
                        logger.warn("Error during checkState for job id={}, keyspace={}, table={}: {}",
                                   id, keyspace, table, checkStateException.getMessage());
                        // Job was created but checkState failed - still return it so it's added to the map
                        // The error will be captured in the job's status
                    }
                    logger.info("Created UrlImportJob for id={}, keyspace={}, table={}", id, keyspace, table);
                    return urlJob;
                }
            }
            catch (Exception e)
            {
                logger.warn("Failed to query system_distributed.remote_import for id={}, keyspace={}, table={}",
                           id, keyspace, table, e);
            }
            return null;
        });
    }
    
    public ImportJob getJob(UUID snapshotId)
    {
        return jobs.get(snapshotId);
    }
    
    public void processImportStateChange(UUID snapshotId, String keyspace, String table, String currentState, String newState)
    {
        logger.info("Processing import state change for snapshot={}, keyspace={}, table={}: {} -> {}", 
                   snapshotId, keyspace, table, currentState, newState);
        
        ImportJob job = getOrCreateJob(snapshotId, keyspace, table);
        
        if (job == null)
        {
            logger.warn("Could not create or find import job for snapshot={}, keyspace={}, table={}", 
                       snapshotId, keyspace, table);
            return;
        }

        if ("staging".equals(newState))
        {
            logger.info("Transitioning job {} to staging for {}.{}", snapshotId, keyspace, table);
            job.checkState();
        }
        else if ("importing".equals(newState))
        {
            logger.info("Triggering import for job {} on {}.{} (transition to importing)", 
                       snapshotId, keyspace, table);
            job.checkState();
        }
        else
        {
            ImportStatus jobStatus = job.status.get();
            if (jobStatus == ImportStatus.ERROR || jobStatus == ImportStatus.CANCELLED)
            {
                logger.info("Restarting import job {} in {} state for {}.{}", snapshotId, jobStatus, keyspace, table);
                job.reset();
                job.checkState();
            }
        }
    }
    
    public Map<UUID, ImportJob> getAllJobs()
    {
        return new ConcurrentHashMap<>(jobs);
    }
    
    /**
     * Cleanup task that runs every hour to remove jobs whose UUIDs no longer exist in system_distributed.remote_import
     * and clean up orphaned import directories
     */
    public void cleanupOrphanedJobs()
    {
        try
        {
            logger.debug("Starting cleanup of orphaned import jobs and directories");
            
            // Get all active job UUIDs from system_distributed.remote_import
            Set<UUID> activeJobIds = ConcurrentHashMap.newKeySet();
            
            try
            {
                String query = "SELECT id FROM system_distributed.remote_import PER PARTITION LIMIT 1";
                UntypedResultSet results = QueryProcessor.execute(query, ConsistencyLevel.QUORUM);
                
                for (UntypedResultSet.Row row : results)
                {
                    UUID id = row.getUUID("id");
                    if (id != null)
                        activeJobIds.add(id);
                }
            }
            catch (Exception e)
            {
                logger.warn("Error querying system_distributed.remote_import for active job IDs: {}", e.getMessage());
                // Fail safe: if we can't read the authoritative source, don't delete anything
                return;
            }
            
            // First, clean up orphaned jobs from the jobs map
            int removedJobsCount = cleanupOrphanedJobsFromMap(activeJobIds);
            
            // Then, clean up orphaned directories
            int removedDirsCount = cleanupOrphanedImportDirectories(activeJobIds);
            
            if (removedJobsCount > 0 || removedDirsCount > 0)
            {
                logger.info("Cleanup completed: removed {} orphaned import jobs and {} orphaned directories", 
                           removedJobsCount, removedDirsCount);
            }
            else
            {
                logger.debug("Cleanup completed: no orphaned jobs or directories found");
            }
        }
        catch (Exception e)
        {
            logger.error("Error during import job cleanup", e);
        }
    }
    
    private int cleanupOrphanedJobsFromMap(Set<UUID> activeJobIds)
    {
        Iterator<Map.Entry<UUID, ImportJob>> iterator = jobs.entrySet().iterator();
        int removedCount = 0;
        
        while (iterator.hasNext())
        {
            Map.Entry<UUID, ImportJob> entry = iterator.next();
            UUID snapshotId = entry.getKey();
            ImportJob job = entry.getValue();
            
            if (!activeJobIds.contains(snapshotId))
            {
                logger.info("Removing orphaned import job {} for {}.{} - no longer exists in system_distributed.remote_import", 
                           snapshotId, job.targetKeyspace, job.targetTable);
                
                // Cancel the job if it's still running
                try
                {
                    job.cancel("Orphaned job cleanup");
                }
                catch (Exception e)
                {
                    logger.warn("Error cancelling orphaned job {}: {}", snapshotId, e.getMessage());
                }
                
                iterator.remove();
                removedCount++;
            }
        }
        
        return removedCount;
    }
    
    private int cleanupOrphanedImportDirectories(Set<UUID> activeJobIds)
    {
        int removedCount = 0;
        
        try
        {
            // Add UUIDs from jobs map to the active set
            activeJobIds.addAll(jobs.keySet());
            
            // Now scan all data directories for imports directories
            for (String keyspaceName : Schema.instance.getUserKeyspaces())
            {
                try
                {
                    KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(keyspaceName);
                    if (keyspaceMetadata == null)
                        continue;
                        
                    // Scan all tables in the keyspace - each table has its own data directories
                    for (TableMetadata tableMetadata : keyspaceMetadata.tables)
                    {
                        try
                        {
                            Directories dirs = new Directories(tableMetadata);
                            
                            for (File dataDir : dirs.getCFDirectories())
                            {
                                File importsDir = new File(dataDir, "imports");
                                if (importsDir.exists() && importsDir.isDirectory())
                                {
                                    removedCount += cleanupImportsDirectory(importsDir, activeJobIds);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            logger.warn("Error scanning imports directories for table {}.{}: {}", 
                                       keyspaceName, tableMetadata.name, e.getMessage());
                        }
                    }
                }
                catch (Exception e)
                {
                    logger.warn("Error scanning imports directories in keyspace {}: {}", keyspaceName, e.getMessage());
                }
            }
        }
        catch (Exception e)
        {
            logger.warn("Error during directory cleanup: {}", e.getMessage());
        }
        
        return removedCount;
    }
    
    private int cleanupImportsDirectory(File importsDir, Set<UUID> activeJobIds)
    {
        int removedCount = 0;
        
        try
        {
            File[] subdirs = importsDir.tryList();
            if (subdirs == null)
                return 0;
                
            for (File subdir : subdirs)
            {
                if (subdir.isDirectory())
                {
                    try
                    {
                        // Try to parse directory name as UUID
                        UUID dirUuid = UUID.fromString(subdir.name());
                        
                        if (!activeJobIds.contains(dirUuid))
                        {
                            // Check if directory is old enough to be safely deleted
                            long dirAge = Clock.Global.currentTimeMillis() - subdir.lastModified();
                            long minAgeMs = TimeUnit.SECONDS.toMillis(DatabaseDescriptor.getImportCleanupMinAgeSeconds());
                            if (dirAge < minAgeMs)
                            {
                                logger.debug("Skipping recent orphaned import directory (age: {}ms): {}", 
                                           dirAge, subdir.absolutePath());
                                continue;
                            }
                            
                            // Check if associated job is still active (importing/staging)
                            ImportJob associatedJob = jobs.get(dirUuid);
                            if (associatedJob != null)
                            {
                                logger.debug("Skipping cleanup of active job {} in state {}", dirUuid, associatedJob.status.get());
                                continue;
                            }
                            
                            logger.info("Removing orphaned import directory (age: {}ms): {}", 
                                       dirAge, subdir.absolutePath());
                            
                            // Delete directory and all its contents
                            if (deleteDirectoryRecursively(subdir))
                            {
                                removedCount++;
                            }
                            else
                            {
                                logger.warn("Failed to fully delete orphaned import directory: {}", subdir.absolutePath());
                            }
                        }
                    }
                    catch (IllegalArgumentException e)
                    {
                        // Not a valid UUID, might be some other directory - leave it alone
                        logger.debug("Skipping non-UUID directory in imports: {}", subdir.name());
                    }
                }
            }
        }
        catch (Exception e)
        {
            logger.warn("Error cleaning up imports directory {}: {}", importsDir.absolutePath(), e.getMessage());
        }
        
        return removedCount;
    }
    
    private boolean deleteDirectoryRecursively(File dir)
    {
        try
        {
            File[] files = dir.tryList();
            if (files != null)
            {
                for (File file : files)
                {
                    if (file.isDirectory())
                    {
                        if (!deleteDirectoryRecursively(file))
                            return false;
                    }
                    else
                    {
                        if (!file.tryDelete())
                        {
                            logger.warn("Failed to delete file: {}", file.absolutePath());
                            return false;
                        }
                    }
                }
            }

            return dir.tryDelete();
        }
        catch (Exception e)
        {
            logger.warn("Error during recursive deletion of {}: {}", dir.absolutePath(), e.getMessage());
            return false;
        }
    }

    // ImportJobManagerMBean implementation

    @Override
    public int getImportConcurrency()
    {
        return DatabaseDescriptor.getImportConcurrency();
    }

    @Override
    public int getImportMaxDiskPercentage()
    {
        return DatabaseDescriptor.getImportMaxDiskPercentage();
    }

    @Override
    public int getImportHttpRetryMaxAttempts()
    {
        return DatabaseDescriptor.getImportHttpRetryMaxAttempts();
    }

    @Override
    public void setImportHttpRetryMaxAttempts(int maxAttempts)
    {
        DatabaseDescriptor.setImportHttpRetryMaxAttempts(maxAttempts);
        logger.info("Updated import HTTP retry max attempts to {}", maxAttempts);
    }

    @Override
    public double getImportHttpRetryBackoffMultiplier()
    {
        return DatabaseDescriptor.getImportHttpRetryBackoffMultiplier();
    }

    @Override
    public void setImportHttpRetryBackoffMultiplier(double backoffMultiplier)
    {
        DatabaseDescriptor.setImportHttpRetryBackoffMultiplier(backoffMultiplier);
        logger.info("Updated import HTTP retry backoff multiplier to {}", backoffMultiplier);
    }

    @Override
    public int getImportHttpRetryInitialDelayMs()
    {
        return DatabaseDescriptor.getImportHttpRetryInitialDelayMs();
    }

    @Override
    public void setImportHttpRetryInitialDelayMs(int initialDelayMs)
    {
        DatabaseDescriptor.setImportHttpRetryInitialDelayInMs(initialDelayMs);
        logger.info("Updated import HTTP retry initial delay to {}ms", initialDelayMs);
    }

    @Override
    public int getImportHttpRetryMaxDelayMs()
    {
        return DatabaseDescriptor.getImportHttpRetryMaxDelayMs();
    }

    @Override
    public void setImportHttpRetryMaxDelayMs(int maxDelayMs)
    {
        DatabaseDescriptor.setImportHttpRetryMaxDelayInMs(maxDelayMs);
        logger.info("Updated import HTTP retry max delay to {}ms", maxDelayMs);
    }

    @Override
    public int getImportHttpRetryJitterPercentage()
    {
        return DatabaseDescriptor.getImportHttpRetryJitterPercentage();
    }

    @Override
    public void setImportHttpRetryJitterPercentage(int jitterPercentage)
    {
        DatabaseDescriptor.setImportHttpRetryJitterPercentage(jitterPercentage);
        logger.info("Updated import HTTP retry jitter percentage to {}%", jitterPercentage);
    }

    @Override
    public double getImportDiskThroughputBytesPerSec()
    {
        return DatabaseDescriptor.getImportDiskThroughputBytesPerSec();
    }

    @Override
    public void setImportDiskThroughputBytesPerSec(double bytesPerSec)
    {
        DatabaseDescriptor.setImportDiskThroughputBytesPerSec((long) bytesPerSec);
        logger.info("Updated import disk throughput limit to {} bytes/sec", bytesPerSec);
    }

    @Override
    public int getImportCleanupInitialDelaySeconds()
    {
        return DatabaseDescriptor.getImportCleanupInitialDelaySeconds();
    }

    @Override
    public int getImportCleanupPeriodSeconds()
    {
        return DatabaseDescriptor.getImportCleanupPeriodSeconds();
    }

    @Override
    public int getImportCleanupMinAgeSeconds()
    {
        return DatabaseDescriptor.getImportCleanupMinAgeSeconds();
    }

    @Override
    public int getActiveJobCount()
    {
        return jobs.size();
    }

    @Override
    public void setImportConcurrency(int concurrency)
    {
        DatabaseDescriptor.setImportConcurrency(concurrency);
        DownloadUnzipStep.resizeUnzipPool(concurrency);
        logger.info("Updated import concurrency to {} and resized unzip pool", concurrency);
    }

    @Override
    public void setImportMaxDiskPercentage(int percentage)
    {
        DatabaseDescriptor.setImportMaxDiskPercentage(percentage);
        logger.info("Updated import max disk percentage to {}%", percentage);
    }

    @Override
    public void setImportCleanupInitialDelaySeconds(int seconds)
    {
        DatabaseDescriptor.setImportCleanupInitialDelaySeconds(seconds);
        rescheduleCleanupTask();
        logger.info("Updated import cleanup initial delay to {}s and rescheduled cleanup task", seconds);
    }

    @Override
    public void setImportCleanupPeriodSeconds(int seconds)
    {
        DatabaseDescriptor.setImportCleanupPeriodSeconds(seconds);
        rescheduleCleanupTask();
        logger.info("Updated import cleanup period to {}s and rescheduled cleanup task", seconds);
    }

    @Override
    public void setImportCleanupMinAgeSeconds(int seconds)
    {
        DatabaseDescriptor.setImportCleanupMinAgeSeconds(seconds);
        logger.info("Updated import cleanup min age to {}s", seconds);
    }

    @Override
    public Map<String, String> getActiveJobs()
    {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<UUID, ImportJob> e : jobs.entrySet())
        {
            ImportJob job = e.getValue();
            Map<String, String> s = job.getStatusMap();
            String summary = String.format("%s.%s | %s | step=%s | progress=%s",
                                           job.targetKeyspace,
                                           job.targetTable,
                                           job.status.get(),
                                           s.getOrDefault("step", "?"),
                                           s.getOrDefault("progress", "?"));
            out.put(e.getKey().toString(), summary);
        }
        return out;
    }

    @Override
    public Map<String, String> getJobStatus(String jobId)
    {
        ImportJob job = jobs.get(UUID.fromString(jobId));
        if (job == null)
            return new LinkedHashMap<>();
        Map<String, String> s = new LinkedHashMap<>();
        s.put("id", job.id.toString());
        s.put("keyspace", job.targetKeyspace);
        s.put("table", job.targetTable);
        s.put("status", String.valueOf(job.status.get()));
        s.putAll(job.getStatusMap());
        return s;
    }

    @Override
    public boolean cancelJob(String jobId)
    {
        ImportJob job = jobs.get(UUID.fromString(jobId));
        if (job == null)
            return false;
        job.cancel("Cancelled via nodetool");
        return true;
    }

    @Override
    public Map<String, String> getConfiguration()
    {
        Map<String, String> c = new LinkedHashMap<>();
        c.put("import_concurrency", String.valueOf(getImportConcurrency()));
        c.put("import_max_disk_percentage", String.valueOf(getImportMaxDiskPercentage()));
        c.put("import_disk_throughput_bytes_per_sec", String.valueOf(getImportDiskThroughputBytesPerSec()));
        c.put("import_http_retry_max_attempts", String.valueOf(getImportHttpRetryMaxAttempts()));
        c.put("import_http_retry_initial_delay_ms", String.valueOf(getImportHttpRetryInitialDelayMs()));
        c.put("import_http_retry_backoff_multiplier", String.valueOf(getImportHttpRetryBackoffMultiplier()));
        c.put("import_http_retry_max_delay_ms", String.valueOf(getImportHttpRetryMaxDelayMs()));
        c.put("import_http_retry_jitter_percentage", String.valueOf(getImportHttpRetryJitterPercentage()));
        c.put("import_cleanup_initial_delay_seconds", String.valueOf(getImportCleanupInitialDelaySeconds()));
        c.put("import_cleanup_period_seconds", String.valueOf(getImportCleanupPeriodSeconds()));
        c.put("import_cleanup_min_age_seconds", String.valueOf(getImportCleanupMinAgeSeconds()));
        c.put("active_job_count", String.valueOf(getActiveJobCount()));
        return c;
    }

    @Override
    public void setConfiguration(String name, String value)
    {
        if (name == null || value == null)
            throw new IllegalArgumentException("name and value must be non-null");
        switch (name)
        {
            case "import_concurrency":
                setImportConcurrency(Integer.parseInt(value));
                return;
            case "import_max_disk_percentage":
                setImportMaxDiskPercentage(Integer.parseInt(value));
                return;
            case "import_disk_throughput_bytes_per_sec":
                setImportDiskThroughputBytesPerSec(Double.parseDouble(value));
                return;
            case "import_http_retry_max_attempts":
                setImportHttpRetryMaxAttempts(Integer.parseInt(value));
                return;
            case "import_http_retry_initial_delay_ms":
                setImportHttpRetryInitialDelayMs(Integer.parseInt(value));
                return;
            case "import_http_retry_backoff_multiplier":
                setImportHttpRetryBackoffMultiplier(Double.parseDouble(value));
                return;
            case "import_http_retry_max_delay_ms":
                setImportHttpRetryMaxDelayMs(Integer.parseInt(value));
                return;
            case "import_http_retry_jitter_percentage":
                setImportHttpRetryJitterPercentage(Integer.parseInt(value));
                return;
            case "import_cleanup_initial_delay_seconds":
                setImportCleanupInitialDelaySeconds(Integer.parseInt(value));
                return;
            case "import_cleanup_period_seconds":
                setImportCleanupPeriodSeconds(Integer.parseInt(value));
                return;
            case "import_cleanup_min_age_seconds":
                setImportCleanupMinAgeSeconds(Integer.parseInt(value));
                return;
            default:
                throw new IllegalArgumentException("Unknown import config key: " + name);
        }
    }
}