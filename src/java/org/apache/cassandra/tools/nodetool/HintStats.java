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

import java.util.List;
import java.util.Map;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "hintstats", description = "Print hint delivery statistics per endpoint")
public class HintStats extends NodeTool.NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        double currentThrottle = probe.getCurrentHintsThrottleInKiB();
        int baseThrottle = probe.getHintedHandoffThrottleInKB();
        int maxThrottle = probe.getHintedHandoffMaxThrottleInKB();

        probe.output().out.println("Hints Throttle Status:");
        probe.output().out.println("  Current Throttle: " + String.format("%.2f KB/s", currentThrottle));
        probe.output().out.println("  Base Throttle: " + baseThrottle + " KB/s");
        probe.output().out.println("  Max Throttle: " + maxThrottle + " KB/s");
        probe.output().out.println();

        List<Map<String, String>> metrics = probe.getHintDeliveryMetrics();

        if (metrics.isEmpty())
        {
            probe.output().out.println("No hint delivery metrics available");
            return;
        }

        TableBuilder tableBuilder = new TableBuilder();
        tableBuilder.add("Endpoint", "Succeeded", "Failed", "Timedout", "Success Rate/s", "Throughput KB/s");

        for (Map<String, String> m : metrics)
        {
            String endpoint = m.get("endpoint");
            String succeeded = m.getOrDefault("succeeded", "0");
            String failed = m.getOrDefault("failed", "0");
            String timedout = m.getOrDefault("timedout", "0");
            String successRate = m.getOrDefault("success_rate", "0.00");

            double throughputRate = Double.parseDouble(m.getOrDefault("throughput_rate", "0"));
            String throughputKBps = String.format("%.2f", throughputRate / 1024);

            tableBuilder.add(endpoint, succeeded, failed, timedout, successRate, throughputKBps);
        }

        tableBuilder.printTo(probe.output().out);
    }
}