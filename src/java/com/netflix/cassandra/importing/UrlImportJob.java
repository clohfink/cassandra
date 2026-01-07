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

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.steps.ValidationStep;

/**
 * URL-based import job that downloads SSTable files from remote URLs and imports them into Cassandra.
 * 
 * <p>This job follows a state machine pattern with internal step transitions and external state coordination:</p>
 * 
 * <h3>Internal Step Flow</h3>
 * <pre>
 * VALIDATING → FILTERING → DOWNLOADING → UNZIPPING → STAGED → IMPORTING → TRIMMING → DONE
 *                                                     ↓
 *                                        (waits for external state change)
 * </pre>
 * 
 * <h3>External State Coordination</h3>
 * <p>The system_distributed.remote_import table manages coordination with automated and manual state transitions:</p>
 * <ul>
 * <li><b>pending</b> - User sets this to start job processing (triggers ImportJobManager)</li>
 * <li><b>staging</b> - Automatically set by job after validation, indicates active processing</li>
 * <li><b>staged</b> - Automatically set when downloads/extraction complete, ready for import</li>
 * <li><b>importing</b> - User sets this to trigger actual SSTable import across all nodes</li>
 * </ul>
 * 
 * <h3>Internal Step Details</h3>
 * <ul>
 * <li><b>VALIDATING</b> - Initial state when job is created, validates target keyspace and table exist</li>
 * <li><b>FILTERING</b> - Filters import sources based on owned token ranges and datacenter filters</li>
 * <li><b>DOWNLOADING</b> - Downloads files from URLs in parallel with disk space monitoring</li>
 * <li><b>UNZIPPING</b> - Extracts downloaded ZIP archives with compression tracking</li>
 * <li><b>STAGED</b> - Sets state to "staged", waits for user to set state to "importing"</li>
 * <li><b>IMPORTING</b> - Loads SSTable files into Cassandra</li>
 * <li><b>TRIMMING</b> - Cleans up data outside owned token ranges</li>
 * <li><b>DONE</b> - Final state indicating successful completion</li>
 * </ul>
 * 
 * <h3>Multi-Node Coordination Workflow</h3>
 * <ol>
 * <li><b>User action:</b> Sets system_distributed.remote_import state to "pending"</li>
 * <li><b>Automated:</b> RemoteImportTrigger notifies all nodes, jobs start processing</li>
 * <li><b>Automated:</b> Job sets state to "staging" after validation</li>
 * <li><b>Automated:</b> Job processes through download/unzip phases</li>
 * <li><b>Automated:</b> Job sets state to "staged" when ready for import</li>
 * <li><b>User action:</b> Sets state to "importing" when all nodes ready</li>
 * <li><b>Automated:</b> RemoteImportTrigger notifies all nodes, jobs proceed to import</li>
 * </ol>
 * 
 * <h3>Key Features</h3>
 * <ul>
 * <li>Token range filtering for distributed imports</li>
 * <li>Parallel HTTP/2 downloads with progress tracking</li>
 * <li>Disk space monitoring and automatic cleanup</li>
 * <li>Multi-node coordination via external state management</li>
 * <li>Comprehensive metrics collection</li>
 * <li>Atomic error handling with resource cleanup</li>
 * </ul>
 * 
 * <h3>Error Handling</h3>
 * <p>The job can transition to ERROR state from any stage on unrecoverable errors such as:</p>
 * <ul>
 * <li>Validation failures (missing keyspace/table)</li>
 * <li>Download failures (HTTP errors, network issues)</li>
 * <li>Disk space exhaustion</li>
 * <li>Import failures</li>
 * </ul>
 * 
 * <p>The job can be cancelled, transitioning to CANCELLED state with automatic cleanup of:</p>
 * <ul>
 * <li>Pending downloads</li>
 * <li>Staging directory and temporary files</li>
 * <li>Resource handles and metrics tracking</li>
 * </ul>
 */
public class UrlImportJob extends ImportJob
{
    private static final Logger logger = LoggerFactory.getLogger(UrlImportJob.class);

    public UrlImportJob(UUID id, String targetKeyspace, String targetTable)
    {
        super(id, targetKeyspace, targetTable);
    }

    @Override
    protected ImportStep doCreateInitialStep()
    {
        return new ValidationStep(id, targetKeyspace, targetTable);
    }

}