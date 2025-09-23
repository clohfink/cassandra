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
package org.apache.cassandra.tools.nodetool;

import java.util.*;
import java.text.DecimalFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.utils.FBUtilities;

@Command(name = "incrementalrepairstats", description = "Print incremental repair statistics for tables")
public class IncrementalRepairStats extends NodeToolCmd
{
    @Arguments(usage = "[<keyspace.table>...]", description = "List of tables (or keyspace) names")
    private List<String> tableNames = new ArrayList<>();

    @Option(name = "-i", description = "Ignore the list of tables and display the remaining tables")
    private boolean ignore = false;

    @Option(title = "human_readable",
    name = {"-H", "--human-readable"},
    description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB")
    private boolean humanReadable = false;

    @Option(title = "format",
    name = {"-F", "--format"},
    description = "Output format (json)")
    private String outputFormat = "";

    private static final DecimalFormat PERCENT_FORMAT = new DecimalFormat("#0.00");

    @Override
    public void execute(NodeProbe probe)
    {
        if (!outputFormat.isEmpty() && !"json".equals(outputFormat))
        {
            throw new IllegalArgumentException("argument for -F can only be json.");
        }
        List<TableData> tables = new ArrayList<>();
        Set<String> specifiedTables = new HashSet<>();

        // Process tableNames argument
        for (String tableSpec : tableNames)
        {
            if (tableSpec.contains("."))
            {
                specifiedTables.add(tableSpec);
            }
            else
            {
                // It's a keyspace name, we'll add all tables later
                specifiedTables.add(tableSpec);
            }
        }

        // Iterate through all table MBeans
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> tableMBeans = probe.getColumnFamilyStoreMBeanProxies();
        while (tableMBeans.hasNext())
        {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = tableMBeans.next();
            String keyspaceName = entry.getKey();
            ColumnFamilyStoreMBean store = entry.getValue();
            String tableName = store.getTableName();
            String fullTableName = keyspaceName + "." + tableName;
            // Check if we should include this table
            boolean shouldInclude;
            if (tableNames.isEmpty())
            {
                shouldInclude = !ignore;
            }
            else
            {
                boolean isSpecified = specifiedTables.contains(fullTableName) || specifiedTables.contains(keyspaceName);
                shouldInclude = ignore ? !isSpecified : isSpecified;
            }

            if (shouldInclude && isIncrementalRepairEnabled(probe, keyspaceName, tableName))
            {
                try
                {
                    TableData tableData = new TableData();
                    tableData.keyspaceName = keyspaceName;
                    tableData.tableName = tableName;
                    tableData.fullName = fullTableName;
                    tableData.bytesRepaired = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesRepaired");
                    tableData.bytesUnrepaired = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesUnrepaired");
                    tableData.bytesPendingRepair = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "BytesPendingRepair");
                    tableData.unrepairedAge = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "UnrepairedAgeInSeconds");

                    if (tableData.bytesRepaired != null && tableData.bytesUnrepaired != null && tableData.bytesPendingRepair != null)
                    {
                        tables.add(tableData);
                    }
                }
                catch (Exception e)
                {
                    // Skip tables that can't be accessed
                }
            }
        }

        if (tables.isEmpty())
        {
            if ("json".equals(outputFormat))
            {
                probe.output().out.println("{}");
            }
            else
            {
                probe.output().out.println("No tables with incremental repair enabled found.");
            }
            return;
        }

        if ("json".equals(outputFormat))
        {
            printJson(probe, tables);
        }
        else
        {
            printTabular(probe, tables);
        }
    }

    private void printTabular(NodeProbe probe, List<TableData> tables)
    {
        // Print header
        probe.output().out.printf("%-50s %15s %15s %15s %10s %10s %15s%n",
                                  "Table", "Bytes Repaired", "Bytes Unrepaired", "Bytes Pending", "% Repaired", "% Unrepaired", "Unrepaired Age");

        long totalBytesRepaired = 0;
        long totalBytesUnrepaired = 0;
        long totalBytesPending = 0;
        long oldestUnrepaired = Long.MAX_VALUE;

        for (TableData tableData : tables)
        {
            long bytesRepaired = tableData.bytesRepaired != null ? tableData.bytesRepaired : 0;
            long bytesUnrepaired = tableData.bytesUnrepaired != null ? tableData.bytesUnrepaired : 0;
            long bytesPending = tableData.bytesPendingRepair != null ? tableData.bytesPendingRepair : 0;
            long unrepairedAge = tableData.unrepairedAge != null ? tableData.unrepairedAge : 0;

            long totalBytes = bytesRepaired + bytesUnrepaired + bytesPending;
            double percentRepaired = totalBytes > 0 ? (double) bytesRepaired / totalBytes * 100 : 100.0;
            double percentUnrepaired = totalBytes > 0 ? (double) bytesUnrepaired / totalBytes * 100 : 0.0;

            totalBytesRepaired += bytesRepaired;
            totalBytesUnrepaired += bytesUnrepaired;
            totalBytesPending += bytesPending;

            if (unrepairedAge > 0 && unrepairedAge < oldestUnrepaired)
            {
                oldestUnrepaired = unrepairedAge;
            }

            probe.output().out.printf("%-50s %15s %15s %15s %9s%% %9s%% %15s%n",
                                      tableData.fullName,
                                      formatBytes(bytesRepaired, humanReadable),
                                      formatBytes(bytesUnrepaired, humanReadable),
                                      formatBytes(bytesPending, humanReadable),
                                      PERCENT_FORMAT.format(percentRepaired),
                                      PERCENT_FORMAT.format(percentUnrepaired),
                                      formatAge(unrepairedAge));
        }

        // Print totals
        probe.output().out.println();
        probe.output().out.println("Totals:");
        long grandTotal = totalBytesRepaired + totalBytesUnrepaired + totalBytesPending;
        double totalPercentRepaired = grandTotal > 0 ? (double) totalBytesRepaired / grandTotal * 100 : 100.0;
        double totalPercentUnrepaired = grandTotal > 0 ? (double) totalBytesUnrepaired / grandTotal * 100 : 0.0;

        probe.output().out.printf("%-50s %15s %15s %15s %9s%% %9s%% %15s%n",
                                  "TOTAL",
                                  formatBytes(totalBytesRepaired, humanReadable),
                                  formatBytes(totalBytesUnrepaired, humanReadable),
                                  formatBytes(totalBytesPending, humanReadable),
                                  PERCENT_FORMAT.format(totalPercentRepaired),
                                  PERCENT_FORMAT.format(totalPercentUnrepaired),
                                  oldestUnrepaired != Long.MAX_VALUE ? formatAge(oldestUnrepaired) : "N/A");
    }

    private void printJson(NodeProbe probe, List<TableData> tables)
    {
        try
        {
            Map<String, Object> output = new LinkedHashMap<>();
            List<Map<String, Object>> tableList = new ArrayList<>();

            long totalBytesRepaired = 0;
            long totalBytesUnrepaired = 0;
            long totalBytesPending = 0;
            long oldestUnrepaired = Long.MAX_VALUE;

            for (TableData tableData : tables)
            {
                long bytesRepaired = tableData.bytesRepaired != null ? tableData.bytesRepaired : 0;
                long bytesUnrepaired = tableData.bytesUnrepaired != null ? tableData.bytesUnrepaired : 0;
                long bytesPending = tableData.bytesPendingRepair != null ? tableData.bytesPendingRepair : 0;
                long unrepairedAge = tableData.unrepairedAge != null ? tableData.unrepairedAge : 0;

                long totalBytes = bytesRepaired + bytesUnrepaired + bytesPending;
                double percentRepaired = totalBytes > 0 ? (double) bytesRepaired / totalBytes * 100 : 100.0;
                double percentUnrepaired = totalBytes > 0 ? (double) bytesUnrepaired / totalBytes * 100 : 0.0;

                totalBytesRepaired += bytesRepaired;
                totalBytesUnrepaired += bytesUnrepaired;
                totalBytesPending += bytesPending;

                if (unrepairedAge > 0 && unrepairedAge < oldestUnrepaired)
                {
                    oldestUnrepaired = unrepairedAge;
                }

                Map<String, Object> tableJson = new LinkedHashMap<>();
                tableJson.put("keyspace", tableData.keyspaceName);
                tableJson.put("table", tableData.tableName);
                tableJson.put("bytes_repaired", bytesRepaired);
                tableJson.put("bytes_unrepaired", bytesUnrepaired);
                tableJson.put("bytes_pending_repair", bytesPending);
                tableJson.put("percent_repaired", Double.parseDouble(PERCENT_FORMAT.format(percentRepaired)));
                tableJson.put("percent_unrepaired", Double.parseDouble(PERCENT_FORMAT.format(percentUnrepaired)));
                tableJson.put("unrepaired_age_seconds", unrepairedAge);

                if (humanReadable)
                {
                    tableJson.put("bytes_repaired_human", formatBytes(bytesRepaired, true));
                    tableJson.put("bytes_unrepaired_human", formatBytes(bytesUnrepaired, true));
                    tableJson.put("bytes_pending_repair_human", formatBytes(bytesPending, true));
                    tableJson.put("unrepaired_age_human", formatAge(unrepairedAge));
                }

                tableList.add(tableJson);
            }

            // Add totals
            long grandTotal = totalBytesRepaired + totalBytesUnrepaired + totalBytesPending;
            double totalPercentRepaired = grandTotal > 0 ? (double) totalBytesRepaired / grandTotal * 100 : 100.0;
            double totalPercentUnrepaired = grandTotal > 0 ? (double) totalBytesUnrepaired / grandTotal * 100 : 0.0;

            Map<String, Object> totals = new LinkedHashMap<>();
            totals.put("bytes_repaired", totalBytesRepaired);
            totals.put("bytes_unrepaired", totalBytesUnrepaired);
            totals.put("bytes_pending_repair", totalBytesPending);
            totals.put("percent_repaired", Double.parseDouble(PERCENT_FORMAT.format(totalPercentRepaired)));
            totals.put("percent_unrepaired", Double.parseDouble(PERCENT_FORMAT.format(totalPercentUnrepaired)));
            totals.put("oldest_unrepaired_age_seconds", oldestUnrepaired != Long.MAX_VALUE ? oldestUnrepaired : null);

            if (humanReadable)
            {
                totals.put("bytes_repaired_human", formatBytes(totalBytesRepaired, true));
                totals.put("bytes_unrepaired_human", formatBytes(totalBytesUnrepaired, true));
                totals.put("bytes_pending_repair_human", formatBytes(totalBytesPending, true));
                totals.put("oldest_unrepaired_age_human", oldestUnrepaired != Long.MAX_VALUE ? formatAge(oldestUnrepaired) : null);
            }

            output.put("tables", tableList);
            output.put("totals", totals);

            ObjectMapper mapper = new ObjectMapper();
            probe.output().out.println(mapper.writeValueAsString(output));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error generating JSON output", e);
        }
    }

    private boolean isIncrementalRepairEnabled(NodeProbe probe, String keyspaceName, String tableName)
    {
        try
        {
            // Check if incremental repair is enabled by looking at unrepaired age metric
            // If the metric exists and is >= 0, incremental repair is enabled
            Long unrepairedAge = (Long) probe.getColumnFamilyMetric(keyspaceName, tableName, "UnrepairedAgeInSeconds");
            return unrepairedAge != null && unrepairedAge > 0;
        }
        catch (Exception e)
        {
            return false;
        }
    }

    private static class TableData
    {
        String keyspaceName;
        String tableName;
        String fullName;
        Long bytesRepaired;
        Long bytesUnrepaired;
        Long bytesPendingRepair;
        Long unrepairedAge;
    }

    private String formatBytes(long bytes, boolean humanReadable)
    {
        if (!humanReadable)
        {
            return String.valueOf(bytes);
        }
        return FBUtilities.prettyPrintMemory(bytes);
    }

    private String formatAge(long ageInSeconds)
    {
        if (ageInSeconds <= 0)
        {
            return "0";
        }

        long days = ageInSeconds / 86400;
        long hours = (ageInSeconds % 86400) / 3600;
        long minutes = (ageInSeconds % 3600) / 60;
        long seconds = ageInSeconds % 60;

        if (days > 0)
        {
            return String.format("%dd %dh %dm", days, hours, minutes);
        }
        else if (hours > 0)
        {
            return String.format("%dh %dm", hours, minutes);
        }
        else if (minutes > 0)
        {
            return String.format("%dm %ds", minutes, seconds);
        }
        else
        {
            return String.format("%ds", seconds);
        }
    }
}