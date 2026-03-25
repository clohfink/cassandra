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

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.stats.ConfigCheckHolder;
import org.apache.cassandra.tools.nodetool.stats.ConfigCheckPrinter;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;

@Command(name = "configcheck", description = "Check if the on-disk cassandra.yaml has changed since the node started")
public class ConfigCheck extends NodeToolCmd
{
    @Option(title = "diff", name = {"--diff"}, description = "Show full config diff when configs do not match")
    private boolean showDiff = false;

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

        CompositeData data = probe.getConfigDelta();
        ConfigCheckHolder holder = new ConfigCheckHolder(data, showDiff);
        StatsPrinter<ConfigCheckHolder> printer = ConfigCheckPrinter.from(outputFormat);
        printer.print(holder, probe.output().out);
    }
}
