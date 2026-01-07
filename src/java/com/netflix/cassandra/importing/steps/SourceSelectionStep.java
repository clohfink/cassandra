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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.ImportJob;
import com.netflix.cassandra.importing.ImportStatus;
import com.netflix.cassandra.importing.ImportStep;
import com.netflix.cassandra.importing.RemoteImportRecord;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OwnedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

/**
 * Filters and selects remote import sources based on token range ownership and datacenter preferences.
 * <p>
 * <b>Import Flow:</b> VALIDATING → [<b>FILTERING</b>] → DOWNLOADING → STAGED → IMPORTING → TRIMMING → DONE
 * <p>
 * <b>Input:</b> Job configuration from ValidationStep<br>
 * <b>Output:</b> Map of filtered URLs to their sizes for DownloadStep<br>
 * <b>Execution Type:</b> Synchronous filtering operation
 * <p>
 * <b>init():</b> Queries the remote_import table and applies filtering logic:
 * <ul>
 * <li>Token range overlap with locally owned ranges</li>
 * <li>Datacenter filter matching (local datacenter or NETFLIX_REGION environment variable)</li>
 * </ul>
 * <b>checkComplete():</b> Always transitions immediately after filtering completes successfully
 */
public class SourceSelectionStep implements ImportStep
{
    private static final Logger logger = LoggerFactory.getLogger(SourceSelectionStep.class);
    private final UUID jobId;
    private final String targetKeyspace;
    private final String targetTable;
    private final Map<String, Long> urlSizes = new HashMap<>();

    public SourceSelectionStep(UUID jobId, String targetKeyspace, String targetTable)
    {
        this.jobId = jobId;
        this.targetKeyspace = targetKeyspace;
        this.targetTable = targetTable;
    }

    @Override
    public void init()
    {
        logger.info("Starting source selection for import job {}", jobId);
        OwnedRanges ownedRanges = StorageService.instance.getNormalizedLocalRanges(targetKeyspace);
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();

        List<RemoteImportRecord> results = ImportJob.queryRemoteImport(jobId, targetKeyspace, targetTable);

        String localDatacenter = DatabaseDescriptor.getLocalDataCenter();
        String netflixRegion = System.getenv("NETFLIX_REGION");

        for (RemoteImportRecord record : results)
        {
            String source = record.source;

            // Check datacenter filter first
            String dcFilter = record.dcFilter;

            // If dc_filter is set, check if it matches either local datacenter or NETFLIX_REGION (case-insensitive)
            if (dcFilter != null)
            {
                boolean matches = dcFilter.equalsIgnoreCase(localDatacenter) ||
                                  dcFilter.equalsIgnoreCase(netflixRegion);

                if (!matches)
                {
                    logger.info("Excluding source {} - datacenter filter '{}' doesn't match local datacenter '{}' or Netflix region '{}'",
                                source, dcFilter, localDatacenter, netflixRegion);
                    continue;
                }
            }

            // Safely get start/end tokens, using partitioner min/max if null
            String startTokenStr = record.startToken;
            String endTokenStr = record.endToken;

            // Use partitioner min/max tokens if start/end tokens are not set
            if (startTokenStr == null)
                startTokenStr = partitioner.getMinimumToken().toString();
            if (endTokenStr == null)
                endTokenStr = partitioner.getMaximumToken().toString();

            Token startToken = partitioner.getTokenFactory().fromString(startTokenStr);
            Token endToken = partitioner.getTokenFactory().fromString(endTokenStr);
            Range<Token> sourceRange = new Range<>(startToken, endToken);

            boolean includeSource = false;
            if (ownedRanges.checkForOverlapsRange(Collections.singleton(sourceRange)))
            {
                includeSource = true;
                if (dcFilter != null)
                {
                    String matchType = dcFilter.equalsIgnoreCase(localDatacenter) ? "local datacenter" : "Netflix region";
                    logger.info("Including source {} for token range ({}, {}) and datacenter filter '{}' (matched {})", 
                                source, startTokenStr, endTokenStr, dcFilter, matchType);
                }
                else
                    logger.info("Including source {} for token range ({}, {}) with no datacenter filter", 
                                source, startTokenStr, endTokenStr);
            }
            else
            {
                logger.info("Excluding source {} for token range ({}, {}) - not in owned ranges",
                            source, startTokenStr, endTokenStr);
            }

            if (includeSource)
                urlSizes.put(source, record.size != null ? record.size : 0L);
        }
    }

    @Override
    public ImportStep checkComplete()
    {
        return new DownloadUnzipStep(jobId, targetKeyspace, targetTable, urlSizes);
    }

    @Override
    public ImportStatus getStatus()
    {
        return ImportStatus.FILTERING;
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
        status.put("description", "Filtering import sources based on owned token ranges and datacenter filter");
        return status;
    }
}