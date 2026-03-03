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
package org.apache.cassandra.service;

import java.util.Map;
import java.util.stream.Collectors;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.MaintenanceCheckService.StopResult;

/**
 * Converts {@link StopResult} to JMX {@link CompositeData} for the MBean interface.
 */
public class MaintenanceCheckCompositeData
{
    private static final String[] ROW_NAMES = { "keyspace", "status", "message", "consistency", "required", "alive", "blockedBy" };
    private static final String[] ROW_DESCS = { "keyspace name", "check status", "status message",
                                                "consistency level that would break", "replicas required",
                                                "replicas alive", "nodes blocking the operation" };
    private static final OpenType<?>[] ROW_TYPES = { SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
                                                     SimpleType.STRING, SimpleType.INTEGER, SimpleType.INTEGER,
                                                     SimpleType.STRING };

    private static final String[] RESULT_NAMES = { "verdict", "target", "keyspaces" };
    private static final String[] RESULT_DESCS = { "overall safe/unsafe verdict", "target node address", "per-keyspace results" };

    static final CompositeType ROW_TYPE;
    static final TabularType TABLE_TYPE;
    static final CompositeType RESULT_TYPE;

    static
    {
        try
        {
            ROW_TYPE = new CompositeType("ForKeyspace", "Per-keyspace maintenance check result", ROW_NAMES, ROW_DESCS, ROW_TYPES);
            TABLE_TYPE = new TabularType("Keyspaces", "Per-keyspace results", ROW_TYPE, new String[]{ "keyspace" });
            RESULT_TYPE = new CompositeType("StopResult", "Maintenance check stop result", RESULT_NAMES, RESULT_DESCS,
                                            new OpenType<?>[]{ SimpleType.BOOLEAN, SimpleType.STRING, TABLE_TYPE });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeData from(StopResult result)
    {
        try
        {
            TabularDataSupport table = new TabularDataSupport(TABLE_TYPE);
            for (Map.Entry<String, StopResult.ForKeyspace> entry : result.keyspaceResults.entrySet())
            {
                StopResult.ForKeyspace kr = entry.getValue();
                table.put(new CompositeDataSupport(ROW_TYPE, ROW_NAMES,
                    new Object[]{ entry.getKey(), kr.status.name(), kr.message,
                                  kr.consistency, kr.required, kr.alive,
                                  formatBlockedBy(kr.blockedBy) }));
            }
            return new CompositeDataSupport(RESULT_TYPE, RESULT_NAMES,
                                            new Object[]{ result.verdict, result.target.toString(), table });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException("Failed to build CompositeData for StopResult", e);
        }
    }

    private static String formatBlockedBy(Map<InetAddressAndPort, String> blockedBy)
    {
        if (blockedBy == null || blockedBy.isEmpty())
            return null;
        return blockedBy.entrySet().stream()
                        .map(e -> e.getKey() + " (" + e.getValue() + ")")
                        .collect(Collectors.joining(", "));
    }
}
