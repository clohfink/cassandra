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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.ImportJob;
import com.netflix.cassandra.importing.ImportStatus;
import com.netflix.cassandra.importing.ImportStep;
import com.netflix.cassandra.importing.RemoteImportRecord;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;

/**
 * Waits for external signal to proceed with importing the staged SSTable files.
 * <p>
 * <b>Import Flow:</b> VALIDATING → FILTERING → DOWNLOADING → [<b>STAGED</b>] → IMPORTING → TRIMMING → DONE
 * <p>
 * <b>Input:</b> Staging directory with extracted SSTable files from UnzipStep<br>
 * <b>Output:</b> Ready-to-import state for ImportingStep<br>
 * <b>Execution Type:</b> Synchronous polling with adaptive delay and jitter
 * <p>
 * <b>init():</b> Logs completion of downloads and indicates readiness for import trigger<br>
 * <b>checkComplete():</b> Polls the remote_import table for state change to "importing".
 * Uses adaptive delay based on cluster size (1s-1min) with jitter to prevent thundering herd.
 * Only transitions when external system sets the import state to "importing".
 */
public class StagedStep implements ImportStep
{
    private static final Logger logger = LoggerFactory.getLogger(StagedStep.class);
    private final UUID jobId;
    private final String targetKeyspace;
    private final String targetTable;
    private final File stagingDirectory;

    public StagedStep(UUID jobId, String targetKeyspace, String targetTable, File stagingDirectory)
    {
        this.jobId = jobId;
        this.targetKeyspace = targetKeyspace;
        this.targetTable = targetTable;
        this.stagingDirectory = stagingDirectory;
    }

    @Override
    public void init()
    {
        logger.info("Downloads completed for import job {}, waiting for 'importing'", jobId);
    }

    @Override
    public ImportStep checkComplete() throws Exception
    {
        List<RemoteImportRecord> results = ImportJob.queryRemoteImport(jobId, targetKeyspace, targetTable);

        if (results.isEmpty())
        {
            throw new IllegalStateException("Remote import entry not found for job " + jobId);
        }

        RemoteImportRecord record = results.get(0);
        String currentState = record.state;
        if ("importing".equalsIgnoreCase(currentState))
        {
            logger.info("State changed to 'importing' for import job {}, proceeding with import", jobId);
            return new ImportingStep(jobId, targetKeyspace, targetTable, stagingDirectory);
        }

        // Stay in this step until state is updated externally to 'importing'
        return this;
    }

    @Override
    public ImportStatus getStatus()
    {
        return ImportStatus.STAGED;
    }

    @Override
    public double getProgress()
    {
        return 1.0;
    }

    @Override
    public Map<String, String> toStatusMap()
    {
        Map<String, String> status = baseStatusMap();
        status.put("description", "Files downloaded and extracted, waiting for importing signal to proceed");
        
        // Count SSTables and calculate total bytes
        if (stagingDirectory != null && stagingDirectory.exists())
        {
            try
            {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(targetKeyspace, targetTable);
                if (cfs != null)
                {
                    int sstableCount = 0;
                    long totalBytes = 0;
                    
                    // Scan the staging directory and all subdirectories
                    File[] subdirs = stagingDirectory.tryList();
                    if (subdirs != null)
                    {
                        for (File subdir : subdirs)
                        {
                            if (subdir.isDirectory())
                            {
                                Directories.SSTableLister lister = cfs.getDirectories()
                                    .sstableLister(subdir, Directories.OnTxnErr.IGNORE)
                                    .skipTemporary(true);
                                Map<Descriptor, Set<Component>> sstables = lister.list(true);
                                sstableCount += sstables.size();
                                
                                // Calculate total size of all SSTable components
                                for (Map.Entry<Descriptor, Set<Component>> entry : sstables.entrySet())
                                {
                                    Descriptor descriptor = entry.getKey();
                                    for (Component component : entry.getValue())
                                    {
                                        File componentFile = new File(descriptor.filenameFor(component));
                                        if (componentFile.exists())
                                        {
                                            totalBytes += componentFile.length();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    status.put("sstables_staged", String.valueOf(sstableCount));
                    status.put("total_bytes_staged", String.valueOf(totalBytes));
                    status.put("total_size_staged", formatBytes(totalBytes));
                }
            }
            catch (Exception e)
            {
                logger.warn("Failed to count staged SSTables: {}", e.getMessage());
                status.put("sstables_staged", "unknown");
                status.put("total_bytes_staged", "unknown");
                status.put("total_size_staged", "unknown");
            }
        }
        else
        {
            status.put("sstables_staged", "0");
            status.put("total_bytes_staged", "0");
            status.put("total_size_staged", "0 B");
        }
        
        return status;
    }
    
    private String formatBytes(long bytes)
    {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "iB";
        return String.format("%.1f %s", bytes / Math.pow(1024, exp), pre);
    }

    @Override
    public long getNextCheckDelayMs()
    {
        int nodeCount = StorageService.instance.getLiveRingMembers(true).size();
        long baseDelayMs = nodeCount * 100L; // 0.1 second per node

        // Clamp between 1 second and 1 minute
        baseDelayMs = Math.max(1000L, Math.min(60000L, baseDelayMs));

        // Add +/- 10% jitter to avoid thundering herd
        double jitter = 0.1 * baseDelayMs * (2 * Math.random() - 1);
        return baseDelayMs + (long) jitter;
    }
}