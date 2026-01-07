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

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

public class ImportStateChangeRequest
{
    public static final IVersionedSerializer<ImportStateChangeRequest> serializer = new Serializer();
    
    public final UUID snapshotId;
    public final String keyspace;
    public final String table;

    public final String currentState;
    public final String newState;
    
    public ImportStateChangeRequest(UUID snapshotId, String keyspace, String table, String currentState, String newState)
    {
        this.snapshotId = snapshotId;
        this.keyspace = keyspace;
        this.table = table;
        this.currentState = currentState;
        this.newState = newState;
    }
    
    private static class Serializer implements IVersionedSerializer<ImportStateChangeRequest>
    {
        @Override
        public void serialize(ImportStateChangeRequest request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.snapshotId, out, version);
            out.writeUTF(request.keyspace);
            out.writeUTF(request.table);
            out.writeUTF(request.currentState != null ? request.currentState : "");
            out.writeUTF(request.newState);
        }
        
        @Override
        public ImportStateChangeRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID snapshotId = UUIDSerializer.serializer.deserialize(in, version);
            String keyspace = in.readUTF();
            String table = in.readUTF();
            String currentState = in.readUTF();
            String newState = in.readUTF();
            return new ImportStateChangeRequest(snapshotId, keyspace, table, 
                                               currentState.isEmpty() ? null : currentState, 
                                               newState);
        }
        
        @Override
        public long serializedSize(ImportStateChangeRequest request, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(request.snapshotId, version);
            size += 2 + request.keyspace.length();
            size += 2 + request.table.length();
            size += 2 + (request.currentState != null ? request.currentState.length() : 0);
            size += 2 + request.newState.length();
            return size;
        }
    }
}