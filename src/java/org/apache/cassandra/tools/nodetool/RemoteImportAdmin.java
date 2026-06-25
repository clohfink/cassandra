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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import com.netflix.cassandra.importing.ImportJobManagerMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * nodetool entry point for working with the remote SSTable import system.
 *
 * <pre>
 *   nodetool remoteimport status                       # list active import jobs on this node
 *   nodetool remoteimport status &lt;jobId&gt;               # detailed status for one job
 *   nodetool remoteimport getconfig                    # dump all hot-tunable configs
 *   nodetool remoteimport setconfig &lt;name&gt; &lt;value&gt;     # set one hot-tunable config
 *   nodetool remoteimport cancel &lt;jobId&gt;               # cancel a single in-flight job
 *   nodetool remoteimport cleanup                      # reap orphaned jobs and imports/ dirs
 * </pre>
 */
public abstract class RemoteImportAdmin extends NodeTool.NodeToolCmd
{
    @Command(name = "status", description = "List active remote import jobs on this node, or detailed status for one job id")
    public static class StatusCmd extends RemoteImportAdmin
    {
        @Arguments(title = "job_id", usage = "[job_id]",
                   description = "Optional UUID of a single job; when omitted, lists all active jobs")
        private List<String> args = new ArrayList<>();

        protected void execute(NodeProbe probe)
        {
            PrintStream out = probe.output().out;
            ImportJobManagerMBean mbean = probe.getImportJobManagerProxy();

            if (args.isEmpty())
            {
                Map<String, String> active = mbean.getActiveJobs();
                if (active.isEmpty())
                {
                    out.println("No active import jobs");
                    return;
                }
                List<List<String>> rows = new ArrayList<>();
                rows.add(Lists.newArrayList("id", "summary"));
                for (Map.Entry<String, String> e : active.entrySet())
                {
                    rows.add(Lists.newArrayList(e.getKey(), e.getValue()));
                }
                printTable(rows, out);
            }
            else
            {
                checkArgument(args.size() == 1, "status takes at most one job_id argument");
                Map<String, String> status = mbean.getJobStatus(args.get(0));
                if (status.isEmpty())
                {
                    out.println("No job with id " + args.get(0) + " on this node");
                    return;
                }
                List<List<String>> rows = new ArrayList<>();
                rows.add(Lists.newArrayList("field", "value"));
                for (Map.Entry<String, String> e : status.entrySet())
                {
                    rows.add(Lists.newArrayList(e.getKey(), String.valueOf(e.getValue())));
                }
                printTable(rows, out);
            }
        }
    }

    @Command(name = "getconfig", description = "Print all hot-tunable remote import configuration values")
    public static class GetConfigCmd extends RemoteImportAdmin
    {
        protected void execute(NodeProbe probe)
        {
            PrintStream out = probe.output().out;
            Map<String, String> config = probe.getImportJobManagerProxy().getConfiguration();
            List<List<String>> rows = new ArrayList<>();
            rows.add(Lists.newArrayList("key", "value"));
            for (Map.Entry<String, String> e : config.entrySet())
            {
                rows.add(Lists.newArrayList(e.getKey(), e.getValue()));
            }
            printTable(rows, out);
        }
    }

    @Command(name = "setconfig", description = "Set a single hot-tunable remote import configuration value")
    public static class SetConfigCmd extends RemoteImportAdmin
    {
        @Arguments(title = "<name> <value>", usage = "<name> <value>",
                   description = "Configuration key and value; valid keys are those listed by `remoteimport getconfig`",
                   required = true)
        private List<String> args = new ArrayList<>();

        protected void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 2, "setconfig requires exactly two arguments: <name> <value>");
            probe.getImportJobManagerProxy().setConfiguration(args.get(0), args.get(1));
            probe.output().out.printf("Set %s = %s%n", args.get(0), args.get(1));
        }
    }

    @Command(name = "cancel", description = "Cancel a single in-flight remote import job by id")
    public static class CancelCmd extends RemoteImportAdmin
    {
        @Arguments(title = "job_id", usage = "<job_id>",
                   description = "UUID of the import job to cancel",
                   required = true)
        private List<String> args = new ArrayList<>();

        protected void execute(NodeProbe probe)
        {
            checkArgument(args.size() == 1, "cancel requires exactly one job_id argument");
            String jobId = args.get(0);
            boolean cancelled = probe.getImportJobManagerProxy().cancelJob(jobId);
            PrintStream out = probe.output().out;
            if (cancelled)
                out.println("Cancelled import job " + jobId);
            else
                out.println("No active import job with id " + jobId + " on this node");
        }
    }

    @Command(name = "cleanup", description = "Reap orphaned import jobs and orphaned imports/ directories")
    public static class CleanupCmd extends RemoteImportAdmin
    {
        protected void execute(NodeProbe probe)
        {
            probe.getImportJobManagerProxy().cleanupOrphanedJobs();
            probe.output().out.println("Triggered orphaned import job cleanup");
        }
    }

    private static void printTable(List<List<String>> rows, PrintStream out)
    {
        if (rows.isEmpty())
            return;
        int[] widths = new int[rows.get(0).size()];
        for (List<String> row : rows)
        {
            for (int i = 0; i < widths.length; i++)
                widths[i] = Math.max(widths[i], row.get(i) == null ? 0 : row.get(i).length());
        }
        StringBuilder fmt = new StringBuilder();
        for (int i = 0; i < widths.length; i++)
            fmt.append("%-").append(widths[i] + 2).append("s");
        fmt.append('%').append('n');
        for (List<String> row : rows)
            out.printf(fmt.toString(), row.toArray());
    }
}
