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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

public class ImportStateChangeVerbHandler implements IVerbHandler<ImportStateChangeRequest>
{
    private static final Logger logger = LoggerFactory.getLogger(ImportStateChangeVerbHandler.class);
    
    public static final ImportStateChangeVerbHandler instance = new ImportStateChangeVerbHandler();
    
    private ImportStateChangeVerbHandler()
    {
    }
    
    @Override
    public void doVerb(Message<ImportStateChangeRequest> message)
    {
        ImportStateChangeRequest request = message.payload;
        
        logger.debug("Received import state change request for snapshot={}, keyspace={}, table={}: {} -> {}", 
                    request.snapshotId, request.keyspace, request.table, request.currentState, request.newState);
        
        try
        {
            ImportJobManager.getInstance().processImportStateChange(
                request.snapshotId, 
                request.keyspace, 
                request.table,
                request.currentState,
                request.newState
            );
        }
        catch (Exception e)
        {
            logger.error("Failed to process import state change for snapshot={}, keyspace={}, table={}", 
                        request.snapshotId, request.keyspace, request.table, e);
        }
    }
}