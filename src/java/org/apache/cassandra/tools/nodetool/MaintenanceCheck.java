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

import javax.management.openmbean.CompositeData;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.stats.MaintenanceCheckHolder;
import org.apache.cassandra.tools.nodetool.stats.MaintenanceCheckPrinter;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;

/**
 * Check if it's safe to take a node down for maintenance without breaking quorum.
 */
public abstract class MaintenanceCheck extends NodeToolCmd
{
    @Command(name = "stop", description = "Check if it's safe to stop a node for maintenance without breaking quorum")
    public static class StopCmd extends MaintenanceCheck
    {
        @Arguments(title = "node", usage = "<node>", description = "Node address (IP[:port] or hostname) to check", required = true)
        private String node;

        @Option(title = "format",
                name = {"-F", "--format"},
                description = "Output format (json, yaml)")
        private String outputFormat = "";

        @Override
        public void execute(NodeProbe probe)
        {
            if (!outputFormat.isEmpty() && !"json".equals(outputFormat) && !"yaml".equals(outputFormat))
            {
                throw new IllegalArgumentException("arguments for -F are json,yaml only.");
            }

            String raw = node.startsWith("/") ? node.substring(1) : node;
            CompositeData data = probe.checkStop(raw);

            MaintenanceCheckHolder holder = new MaintenanceCheckHolder(data);
            StatsPrinter<MaintenanceCheckHolder> printer = MaintenanceCheckPrinter.from(outputFormat);
            printer.print(holder, probe.output().out);
        }
    }
}
