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

public class RemoteImportRecord
{
    public final UUID id;
    public final String targetKeyspace;
    public final String targetTable;
    public final String state;
    public final String source;
    public final String sourceType;
    public final String startToken;
    public final String endToken;
    public final String dcFilter;
    public final Long size;

    public RemoteImportRecord(UUID id, String targetKeyspace, String targetTable, String state,
                              String source, String sourceType, String startToken, String endToken,
                              String dcFilter, Long size)
    {
        this.id = id;
        this.targetKeyspace = targetKeyspace;
        this.targetTable = targetTable;
        this.state = state;
        this.source = source;
        this.sourceType = sourceType;
        this.startToken = startToken;
        this.endToken = endToken;
        this.dcFilter = dcFilter;
        this.size = size;
    }
}
