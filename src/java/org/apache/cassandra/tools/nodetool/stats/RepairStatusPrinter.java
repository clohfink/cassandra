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
package org.apache.cassandra.tools.nodetool.stats;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

public class RepairStatusPrinter
{
    public static StatsPrinter<RepairStatusHolder> from(String format)
    {
        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter<>();
            case "yaml":
                return new StatsPrinter.YamlPrinter<>();
            default:
                return new DefaultPrinter();
        }
    }

    public static class DefaultPrinter implements StatsPrinter<RepairStatusHolder>
    {
        @Override
        public void print(RepairStatusHolder data, PrintStream out)
        {
            Map<String, Object> map = data.convert2Map();

            if (map.containsKey("autoRepairMetrics"))
                printAutoRepairMetrics(out, map.get("autoRepairMetrics"));

            if (map.containsKey("incrementalRepairStats"))
                printIncrementalRepairStats(out, map.get("incrementalRepairStats"));

            if (map.containsKey("consistentSessions"))
                printConsistentSessions(out, map.get("consistentSessions"));

            if (map.containsKey("activeRepairs"))
                printActiveRepairs(out, map.get("activeRepairs"));

            if (map.containsKey("tableRepairConfigs"))
                printTableRepairConfigs(out, map.get("tableRepairConfigs"));
        }

        @SuppressWarnings("unchecked")
        private void printAutoRepairMetrics(PrintStream out, Object section)
        {
            out.println("-- Auto Repair Metrics --");
            List<Map<String, Object>> items = (List<Map<String, Object>>) section;
            if (items.isEmpty())
            {
                out.println("  none");
                out.println();
                return;
            }

            TableBuilder tb = new TableBuilder();
            tb.add("Repair Type", "In Progress", "Node Time(s)", "Cluster Time(s)", "Longest Unrepaired(s)",
                   "Succeeded", "Failed", "Skipped", "My Turn", "Priority Turn", "Force Turn", "MV Tables", "Disabled");
            for (Map<String, Object> item : items)
            {
                tb.add(str(item, "repairType"),
                       str(item, "repairsInProgress"),
                       str(item, "nodeRepairTimeSec"),
                       str(item, "clusterRepairTimeSec"),
                       str(item, "longestUnrepairedSec"),
                       str(item, "succeededTokenRangesCount"),
                       str(item, "failedTokenRangesCount"),
                       str(item, "skippedTokenRangesCount"),
                       str(item, "myTurnCount"),
                       str(item, "myTurnDueToPriority"),
                       str(item, "myTurnForceRepair"),
                       str(item, "totalConsideredMvTables"),
                       str(item, "totalDisabled"));
            }
            tb.printTo(out);
            out.println();
        }

        @SuppressWarnings("unchecked")
        private void printIncrementalRepairStats(PrintStream out, Object section)
        {
            out.println("-- Incremental Repair Stats --");
            List<Map<String, Object>> items = (List<Map<String, Object>>) section;
            if (items.isEmpty())
            {
                out.println("  none");
                out.println();
                return;
            }

            TableBuilder tb = new TableBuilder();
            tb.add("Keyspace", "Table", "Bytes Repaired", "Bytes Unrepaired", "Bytes Pending",
                   "SSTables Repaired", "SSTables Unrepaired", "SSTables Pending");
            for (Map<String, Object> item : items)
            {
                tb.add(str(item, "keyspace"),
                       str(item, "table"),
                       str(item, "bytesRepaired"),
                       str(item, "bytesUnrepaired"),
                       str(item, "bytesPendingRepair"),
                       str(item, "sstablesRepaired"),
                       str(item, "sstablesUnrepaired"),
                       str(item, "sstablesPendingRepair"));
            }
            tb.printTo(out);
            out.println();
        }

        @SuppressWarnings("unchecked")
        private void printConsistentSessions(PrintStream out, Object section)
        {
            out.println("-- Consistent Repair Sessions --");
            List<Map<String, Object>> items = (List<Map<String, Object>>) section;
            if (items.isEmpty())
            {
                out.println("  none");
                out.println();
                return;
            }

            TableBuilder tb = new TableBuilder();
            tb.add("Session ID", "State", "Coordinator", "Participants", "Started", "Last Update");
            for (Map<String, Object> item : items)
            {
                tb.add(str(item, "SESSION_ID"),
                       str(item, "STATE"),
                       str(item, "COORDINATOR"),
                       str(item, "PARTICIPANTS"),
                       str(item, "STARTED"),
                       str(item, "LAST_UPDATE"));
            }
            tb.printTo(out);
            out.println();
        }

        @SuppressWarnings("unchecked")
        private void printActiveRepairs(PrintStream out, Object section)
        {
            out.println("-- Active Repairs --");
            List<Map<String, Object>> items = (List<Map<String, Object>>) section;
            if (items.isEmpty())
            {
                out.println("  none");
                out.println();
                return;
            }

            TableBuilder tb = new TableBuilder();
            tb.add("ID", "Keyspace", "Type", "Status", "Duration(ms)", "Sessions", "Participants", "Ranges", "Failure");
            for (Map<String, Object> item : items)
            {
                tb.add(str(item, "id"),
                       str(item, "keyspaceName"),
                       str(item, "type"),
                       str(item, "status"),
                       str(item, "durationMillis"),
                       str(item, "sessions"),
                       str(item, "participants"),
                       str(item, "ranges"),
                       str(item, "failureCause"));
            }
            tb.printTo(out);
            out.println();
        }

        @SuppressWarnings("unchecked")
        private void printTableRepairConfigs(PrintStream out, Object section)
        {
            out.println("-- Table Repair Configs --");
            List<Map<String, Object>> items = (List<Map<String, Object>>) section;
            if (items.isEmpty())
            {
                out.println("  none");
                out.println();
                return;
            }

            TableBuilder tb = new TableBuilder();
            tb.add("Keyspace", "Table", "Incremental", "Preview", "Full", "Priority");
            for (Map<String, Object> item : items)
            {
                tb.add(str(item, "keyspace"),
                       str(item, "table"),
                       str(item, "incrementalEnabled"),
                       str(item, "previewEnabled"),
                       str(item, "fullEnabled"),
                       str(item, "priority"));
            }
            tb.printTo(out);
            out.println();
        }

        private static String str(Map<String, Object> map, String key)
        {
            Object val = map.get(key);
            return val == null ? "null" : String.valueOf(val);
        }
    }
}
