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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.triggers.TriggerExecutor;

public class RemoteImportTrigger implements ITrigger
{
    private static final Logger logger = LoggerFactory.getLogger(RemoteImportTrigger.class);
    
    CompositeType partitionType = CompositeType.getInstance(UUIDType.instance, UTF8Type.instance, UTF8Type.instance);

    @Override
    public Collection<Mutation> augment(Partition update)
    {
        if (!(update instanceof PartitionUpdate))
        {
            return List.of();
        }
        logger.info("Remote Import partition update {}", update);

        PartitionUpdate partition = (PartitionUpdate) update;
        Row staticRow = partition.staticRow();
        if (staticRow == null)
        {
            return List.of();
        }

        validateStateTransition(partition, staticRow);
        return List.of();
    }

    protected void validateStateTransition(PartitionUpdate partition, Row staticRow)
    {
        staticRow.cells().forEach(cell -> {
            if ("state".equals(cell.column().name.toString()))
            {
                String newState = UTF8Type.instance.compose((ByteBuffer) cell.value()).toLowerCase();
                ByteBuffer[] split = partitionType.split(partition.partitionKey().getKey());
                
                // Validate partition key components before using them
                if (split.length < 3)
                {
                    logger.warn("Invalid partition key structure in remote_import table: expected 3 components, got {}", split.length);
                    return;
                }
                
                if (split[0] == null)
                {
                    logger.warn("Null snapshotId in remote_import table partition key");
                    return;
                }
                
                UUID snapshotId = UUIDType.instance.compose(split[0]);
                if (snapshotId == null)
                {
                    logger.warn("Failed to parse snapshotId from remote_import table partition key");
                    return;
                }
                
                String keyspaceStr = UTF8Type.instance.compose(split[1]);
                String tableStr = UTF8Type.instance.compose(split[2]);

                // Check if ClientState is available and verify permissions
                ClientState clientState = TriggerExecutor.getClientState();
                if (clientState != null)
                {
                    try
                    {
                        // Ensure the client has MODIFY permission on the target keyspace/table
                        clientState.ensureTablePermission(keyspaceStr, tableStr, Permission.MODIFY);
                        logger.debug("Permission check passed for user accessing keyspace={}, table={}", keyspaceStr, tableStr);
                    }
                    catch (UnauthorizedException e)
                    {
                        logger.error("Permission denied for user accessing keyspace={}, table={}: {}", keyspaceStr, tableStr, e.getMessage());
                        throw e;
                    }
                }

                // Get current state from the partition (if available)
                ImportJob job = ImportJobManager.getInstance().getJob(snapshotId);
                String currentState = job == null ? null : job.status.get().toString();
                
                logger.info("State transition detected for snapshot={}, keyspace={}, table={}: {} -> {}", 
                           snapshotId, keyspaceStr, tableStr, currentState, newState);
                
                if (newState.equals("staging") || newState.equals("importing"))
                {
                    sendImportStateChangeRequest(snapshotId, keyspaceStr, tableStr, currentState, newState);
                }
            }
        });
    }

    protected void sendImportStateChangeRequest(UUID snapshotId, String keyspace, String table, String currentState, String newState)
    {
        try
        {
            ImportStateChangeRequest request = new ImportStateChangeRequest(snapshotId, keyspace, table, currentState, newState);
            
            // Send the state change request to all live nodes in the cluster
            Set<InetAddressAndPort> allEndpoints = StorageService.instance.getLiveRingMembers(true);
            for (InetAddressAndPort endpoint : allEndpoints)
            {
                Message<ImportStateChangeRequest> message = Message.out(Verb.IMPORT_STATE_CHANGE_REQ, request);
                MessagingService.instance().send(message, endpoint);
                logger.debug("Sent import state change request to {}", endpoint);
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to send import state change request for snapshot={}, keyspace={}, table={}", 
                        snapshotId, keyspace, table, e);
        }
    }
}
