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

package org.apache.cassandra.distributed.test.netflix;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

/**
 * Comprehensive tests for ping nodetool with message filtering to simulate
 * network failures, timeouts, and various error conditions.
 */
public class PingNodetoolTest extends TestBaseImpl
{
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private static final Pattern TABLE_ROW_PATTERN = 
        Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+:\\d+)\\s+(\\S+)\\s+(UP|DOWN)\\s*(.*)");

    @Test
    public void testPingBasicFunctionality() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                .withConfig(c -> c.with(NETWORK, GOSSIP))
                .start()))
        {
            NodeToolResult tableResult = cluster.get(1).nodetoolResult("ping");
            String tableOutput = tableResult.getStdout();

            assertThat("Expected table header", tableOutput, containsString("Address"));
            assertThat("Expected table header", tableOutput, containsString("Response Time"));
            assertThat("Expected table header", tableOutput, containsString("Status"));

            assertThat("Expected UP status", tableOutput, containsString("UP"));

            NodeToolResult jsonResult = cluster.get(1).nodetoolResult("ping", "--json");
            String jsonOutput = jsonResult.getStdout();
            
            JsonNode jsonData = JSON_MAPPER.readTree(jsonOutput);
            assertNotNull("Should parse as valid JSON", jsonData);
            assertTrue("Should be a JSON object", jsonData.isObject());

            assertFalse("Should have at least one node", jsonData.isEmpty());
        }
    }

    @Test
    public void testPingWithMessageErrors() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                .withConfig(c -> c.with(NETWORK, GOSSIP))
                .start()))
        {
            // Baseline test
            NodeToolResult baselineResult = cluster.get(1).nodetoolResult("ping");
            Map<String, String> baselineResponses = parseTableOutputWithTimes(baselineResult.getStdout());

            assertTrue("Should have multiple nodes in baseline", baselineResponses.size() >= 2);
            for (String response : baselineResponses.values())
            {
                assertTrue("All nodes should show UP in baseline. Got: " + response, response.contains("UP"));
            }
            
            // Drop ECHO_RSP messages from node 2 to node 1 to simulate message failure
            cluster.filters().verbs(Verb.ECHO_RSP.id).from(2).to(1).drop();

            NodeToolResult errorResult = cluster.get(1).nodetoolResult("ping");
            String errorOutput = errorResult.getStdout();

            assertTrue("Should still have output with message errors", !errorOutput.isEmpty());

            Map<String, String> errorResponses = parseTableOutputWithTimes(errorOutput);
            assertTrue("Should still show nodes in output", !errorResponses.isEmpty());

            assertEquals("Should show all 3 nodes even with message errors", 3, errorResponses.size());

            boolean foundWorkingNode = false;
            boolean foundFailingNode = false;
            
            for (Map.Entry<String, String> entry : errorResponses.entrySet())
            {
                String address = entry.getKey();
                String response = entry.getValue();
                
                if (response.contains("UP"))
                {
                    foundWorkingNode = true;
                }
                else if (response.contains("DOWN") || response.contains("timeout") || response.contains("error"))
                {
                    foundFailingNode = true;
                }
            }
            
            assertTrue("Should have at least one working node", foundWorkingNode);
            assertTrue("Should have at least one node with communication failure", foundFailingNode);
        }
    }
    
    @Test
    public void testPingWithInjectedMessageDelay() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                .withConfig(c -> c.with(NETWORK, GOSSIP))
                .start()))
        {
            // Inject 500ms delay in ECHO_RSP messages from node 2 to node 1
            cluster.filters().verbs(Verb.ECHO_RSP.id).from(2).to(1).messagesMatching((from, to, msg) -> {
                try
                {
                    Thread.sleep(500); // 500ms delay
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
                return false;
            }).drop();

            NodeToolResult delayedResult = cluster.get(1).nodetoolResult("ping");
            Map<String, Long> delayedResponseTimes = extractResponseTimes(delayedResult.getStdout());

            assertFalse("Should have delayed response times", delayedResponseTimes.isEmpty());

            // Get baseline response times without any delay
            NodeToolResult baselineResult = cluster.get(1).nodetoolResult("ping");
            Map<String, Long> baselineResponseTimes = extractResponseTimes(baselineResult.getStdout());

            assertFalse("Should have baseline response times", baselineResponseTimes.isEmpty());

            boolean foundDelayedNode = false;
            for (Map.Entry<String, Long> entry : delayedResponseTimes.entrySet())
            {
                String address = entry.getKey();
                long delayedTime = entry.getValue();

                if (baselineResponseTimes.containsKey(address))
                {
                    long baselineTime = baselineResponseTimes.get(address);
                    if (delayedTime > baselineTime)
                    {
                        foundDelayedNode = true;
                    }
                }
            }

            assertTrue("Should find at least one node increased latency due to injected delay",
                      foundDelayedNode);
        }
    }

    // Helper method to parse table output and extract full response data including timing
    private Map<String, String> parseTableOutputWithTimes(String output)
    {
        Map<String, String> nodeResponses = new HashMap<>();
        String[] lines = output.split("\\n");
        
        for (String line : lines)
        {
            Matcher matcher = TABLE_ROW_PATTERN.matcher(line.trim());
            if (matcher.matches())
            {
                String address = matcher.group(1);
                String fullResponse = line.trim();
                nodeResponses.put(address, fullResponse);
            }
        }
        
        return nodeResponses;
    }
    
    // Helper method to extract numerical response times
    private Map<String, Long> extractResponseTimes(String output)
    {
        Map<String, Long> responseTimes = new HashMap<>();
        String[] lines = output.split("\\n");

        for (String line : lines)
        {
            Matcher matcher = TABLE_ROW_PATTERN.matcher(line.trim());
            if (matcher.matches() && matcher.group(3).equals("UP"))
            {
                String address = matcher.group(1);
                String responseTime = matcher.group(2);

                double time = Double.parseDouble(responseTime.replace("us", ""));
                responseTimes.put(address, (long) time);
            }
        }

        return responseTimes;
    }
}