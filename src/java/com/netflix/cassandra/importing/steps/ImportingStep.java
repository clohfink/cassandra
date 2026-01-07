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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.ImportStatus;
import com.netflix.cassandra.importing.ImportStep;
import com.netflix.cassandra.metrics.ImportJobMetrics;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SSTableImporter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;

/**
 * Imports the staged SSTable files into the Cassandra database with detailed failure analysis.
 * <p>
 * <b>Import Flow:</b> VALIDATING → FILTERING → DOWNLOADING → STAGED → [<b>IMPORTING</b>] → TRIMMING → DONE
 * <p>
 * <b>Input:</b> Staging directory with SSTable files from StagedStep<br>
 * <b>Output:</b> Imported SSTables in Cassandra for TrimStep<br>
 * <b>Execution Type:</b> Synchronous SSTable import operation
 * <p>
 * <b>init():</b> Calls ColumnFamilyStore.importNewSSTables() on the staging directory and all subdirectories.
 * If imports fail, performs detailed failure analysis including SSTable validation and provides
 * meaningful error messages for debugging.<br>
 * <b>checkComplete():</b> Always transitions immediately after import completes successfully
 */
public class ImportingStep implements ImportStep
{
    private static final Logger logger = LoggerFactory.getLogger(ImportingStep.class);
    private final UUID jobId;
    private final String targetKeyspace;
    private final String targetTable;
    private final File stagingDirectory;
    private volatile boolean importCompleted = false;

    public ImportingStep(UUID jobId, String targetKeyspace, String targetTable, File stagingDirectory)
    {
        this.jobId = jobId;
        this.targetKeyspace = targetKeyspace;
        this.targetTable = targetTable;
        this.stagingDirectory = stagingDirectory;
    }

    @Override
    public void init()
    {
        try (var timer = ImportJobMetrics.instance.startImportTimer())
        {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(targetKeyspace, targetTable);

            Set<String> importPaths = new HashSet<>();
            importPaths.add(stagingDirectory.absolutePath());
            
            File[] subdirs = stagingDirectory.tryList();
            if (subdirs != null)
            {
                for (File subdir : subdirs)
                {
                    if (subdir.isDirectory())
                    {
                        importPaths.add(subdir.absolutePath());
                    }
                }
            }

            List<String> failed = cfs.importNewSSTables(
            importPaths,
            false, // resetLevel - don't reset compaction level
            true,  // clearRepaired - clear repaired status on imported SSTables
            true,  // verifySSTables - verify SSTable integrity
            false, // verifyTokens - don't verify token ownership (already filtered by range)
            false, // invalidateCaches - don't invalidate caches
            false  // extendedVerify - don't perform extended verification
            );
            if (!failed.isEmpty())
            {
                ImportJobMetrics.instance.sstableImportError(failed.size());
                ImportJobMetrics.instance.importStepError();
                
                String detailedError = analyzeImportFailures(failed);
                throw new RuntimeException(detailedError);
            }
            logger.info("Import completed for job: {}.{}", targetKeyspace, targetTable);
            importCompleted = true;
        }
    }
    
    private String analyzeImportFailures(List<String> failedPaths)
    {
        StringBuilder errorDetails = new StringBuilder();
        errorDetails.append("Failed to import ").append(failedPaths.size()).append(" SSTable(s). Details:\n");
        
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(targetKeyspace, targetTable);
        
        for (String failedPath : failedPaths)
        {
            try
            {
                String pathError = validateSSTablesInPath(failedPath, cfs);
                errorDetails.append(failedPath).append(": ").append(pathError).append("\n");
            }
            catch (Exception e)
            {
                errorDetails.append(failedPath).append(": Could not diagnose failure - ").append(e.getMessage()).append("\n");
            }
        }
        
        return errorDetails.toString();
    }
    
    private String validateSSTablesInPath(String failedPath, ColumnFamilyStore cfs)
    {
        File path = new File(failedPath);
        if (!path.exists())
        {
            return "Path does not exist";
        }
        
        if (!path.isDirectory())
        {
            return "Path is not a directory";
        }
        
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(path, Directories.OnTxnErr.IGNORE).skipTemporary(true);
        Map<Descriptor, Set<Component>> sstables = lister.list(true);
        
        if (sstables.isEmpty())
        {
            return "No valid SSTable files found in directory";
        }
        
        Map.Entry<Descriptor, Set<Component>> firstSSTable = sstables.entrySet().iterator().next();
        Descriptor descriptor = firstSSTable.getKey();
        Set<Component> components = firstSSTable.getValue();
        
        try
        {
            SSTableImporter importer = new SSTableImporter(cfs);
            importer.verifySSTableForImport(descriptor, components, false, true, true);
            return "SSTable validation passed but import failed for unknown reasons";
        }
        catch (Exception e)
        {
            return extractMeaningfulError(e);
        }
    }
    
    private String extractMeaningfulError(Exception e)
    {
        String message = e.getMessage();
        Throwable cause = e.getCause();
        
        if (cause != null && cause.getMessage() != null)
        {
            message = cause.getMessage();
        }
        
        if (message == null)
        {
            message = e.getClass().getSimpleName();
        }
        
        if (message.startsWith("Can't import sstable"))
        {
            int colonIndex = message.indexOf(":");
            if (colonIndex > 0 && colonIndex < message.length() - 1)
            {
                message = message.substring(colonIndex + 1).trim();
            }
        }
        
        return message;
    }

    @Override
    public ImportStep checkComplete()
    {
        return new TrimStep(jobId, targetKeyspace, targetTable);
    }

    @Override
    public ImportStatus getStatus()
    {
        return ImportStatus.IMPORTING;
    }

    @Override
    public double getProgress()
    {
        return importCompleted ? 1.0 : 0.0;
    }

    @Override
    public Map<String, String> toStatusMap()
    {
        Map<String, String> status = baseStatusMap();
        status.put("description", "Loading downloaded SSTables into Cassandra");
        return status;
    }

    @Override
    public void cleanup()
    {
        logger.info("Cleanup requested for importing step in job {}.{}", targetKeyspace, targetTable);
    }
}