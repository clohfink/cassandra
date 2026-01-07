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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.ImportStatus;
import com.netflix.cassandra.importing.ImportStep;

/**
 * Terminal step indicating successful completion of the Netflix Cassandra import process.
 * <p>
 * <b>Import Flow:</b> VALIDATING → FILTERING → DOWNLOADING → STAGED → IMPORTING → TRIMMING → [<b>DONE</b>]
 * <p>
 * <b>Input:</b> Completed import process from TrimStep<br>
 * <b>Output:</b> Terminal state (no further transitions)<br>
 * <b>Execution Type:</b> Terminal state - no operations performed
 * <p>
 * <b>init():</b> No initialization required as this is the final state<br>
 * <b>checkComplete():</b> Returns Long.MAX_VALUE for next check delay (effectively infinite).
 * Always returns self as this is a terminal state. Logs a warning if called as it indicates
 * an unexpected continuation attempt.
 */
public class DoneStep implements ImportStep
{
    private static final Logger logger = LoggerFactory.getLogger(DoneStep.class);
    private final String keyspace;
    private final String table;

    public DoneStep(String keyspace, String table)
    {
        this.keyspace = keyspace;
        this.table = table;
    }

    @Override
    public ImportStep checkComplete()
    {
        logger.warn("Import job {}.{} is already done", keyspace, table);
        return this;
    }

    @Override
    public long getNextCheckDelayMs()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public ImportStatus getStatus()
    {
        return ImportStatus.DONE;
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
        status.put("description", "Import process completed");
        return status;
    }
}
