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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.ImportStatus;
import com.netflix.cassandra.importing.ImportStep;
import com.netflix.cassandra.metrics.ImportJobMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Initial step in the Netflix Cassandra import process that validates the import job configuration.
 * <p>
 * <b>Import Flow:</b> [<b>VALIDATING</b>] → FILTERING → DOWNLOADING → STAGED → IMPORTING → TRIMMING → DONE
 * <p>
 * <b>Input:</b> Job ID, target keyspace, target table, table metadata<br>
 * <b>Output:</b> Transitions to SourceSelectionStep after validation<br>
 * <b>Execution Type:</b> Synchronous initialization
 * <p>
 * <b>init():</b> Validates that the target table metadata exists and is accessible<br>
 * <b>checkComplete():</b> Always transitions immediately to the next step after validation
 */
public class ValidationStep implements ImportStep
{
    private static final Logger logger = LoggerFactory.getLogger(ValidationStep.class);
    private final UUID jobId;
    private final String targetKeyspace;
    private final String targetTable;

    public ValidationStep(UUID jobId, String targetKeyspace, String targetTable)
    {
        this.jobId = jobId;
        this.targetKeyspace = targetKeyspace;
        this.targetTable = targetTable;
    }

    @Override
    public void init()
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(targetKeyspace, targetTable);
        if (metadata == null)
        {
            ImportJobMetrics.instance.validationError();
            ImportJobMetrics.instance.configurationError();
            throw new IllegalArgumentException(String.format("Target keyspace '%s' or table '%s' does not exist",
                                                             targetKeyspace, targetTable));
        }

        logger.info("Import job {} initialized and validating configuration", jobId);
    }

    @Override
    public ImportStep checkComplete()
    {
        return new SourceSelectionStep(jobId, targetKeyspace, targetTable);
    }

    @Override
    public ImportStatus getStatus()
    {
        return ImportStatus.VALIDATING;
    }

    @Override
    public double getProgress()
    {
        return 0.0;
    }

    @Override
    public Map<String, String> toStatusMap()
    {
        Map<String, String> status = baseStatusMap();
        status.put("description", "Import job initialized, waiting to start");
        return status;
    }
}