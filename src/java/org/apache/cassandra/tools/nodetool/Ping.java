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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "ping", description = "Pings all live nodes in gossip and times the response")
public class Ping extends NodeTool.NodeToolCmd
{
    @Option(title = "json", name = {"-j", "--json"}, description = "Displays results in JSON format")
    private boolean json = false;

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            TabularData results = probe.getGossProxy().pingAllNodesWithTiming();
            
            if (json)
            {
                printJsonResults(results, probe);
            }
            else
            {
                printTableResults(results, probe);
            }
        }
        catch (Exception e)
        {
            probe.output().out.println("Error pinging nodes: " + e.getMessage());
        }
    }

    private void printJsonResults(TabularData results, NodeProbe probe)
    {
        Map<String, Object> jsonResults = new LinkedHashMap<>();

        for (Object value : results.values())
        {
            CompositeData compositeData = (CompositeData) value;

            String endpoint = (String) compositeData.get("endpoint");
            Long responseTime = (Long) compositeData.get("response_time");
            String status = (String) compositeData.get("status");
            String reason = (String) compositeData.get("reason");

            Map<String, Object> nodeResult = new LinkedHashMap<>();
            nodeResult.put("response_time_ns", responseTime);
            nodeResult.put("status", status);
            if (reason != null && !reason.isEmpty())
            {
                nodeResult.put("reason", reason);
            }

            jsonResults.put(endpoint, nodeResult);
        }

        try
        {
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonResults);
            probe.output().out.println(json);
        }
        catch (Exception e)
        {
            probe.output().out.println("Error formatting JSON: " + e.getMessage());
        }
    }

    private void printTableResults(TabularData results, NodeProbe probe)
    {
        TableBuilder.SharedTable sharedTable = new TableBuilder.SharedTable("  ");
        TableBuilder tableBuilder = sharedTable.next();

        tableBuilder.add("Address", "Response Time", "Status", "Reason");

        for (Object value : results.values())
        {
            CompositeData compositeData = (CompositeData) value;

            String endpoint = (String) compositeData.get("endpoint");
            Long responseTimeNs = (Long) compositeData.get("response_time");
            String status = (String) compositeData.get("status");
            String reason = (String) compositeData.get("reason");

            String responseTime = status.equals("UP") ? TimeUnit.NANOSECONDS.toMicros(responseTimeNs) + "us" : "N/A";
            String reasonDisplay = (reason != null && !reason.isEmpty()) ? reason : "";

            tableBuilder.add(endpoint, responseTime, status, reasonDisplay);
        }

        for (TableBuilder table : sharedTable.complete())
        {
            table.printTo(probe.output().out);
        }
    }
}