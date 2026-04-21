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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.service.StorageProxyMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.stats.StatsHolder;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;

/**
 * Manage the partition denylist: add, remove, refresh, and list denylisted partition keys.
 * Backed by {@link StorageProxyMBean} denylist operations.
 */
public abstract class Denylist extends NodeToolCmd
{
    @Command(name = "add", description = "Add a partition key to the denylist")
    public static class AddCmd extends Denylist
    {
        @Arguments(usage = "<keyspace> <table> <partition_key>",
                   description = "Keyspace, table, and string form of the partition key to deny",
                   required = true)
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            if (args.size() != 3)
                throw new IllegalArgumentException("denylist add requires exactly 3 arguments: <keyspace> <table> <partition_key>");

            final String keyspace = args.get(0);
            final String table = args.get(1);
            final String key = args.get(2);
            final PrintStream out = probe.output().out;

            if (probe.getSpProxy().denylistKey(keyspace, table, key))
                out.printf("Denylisted %s in %s.%s%n", key, keyspace, table);
            else
                throw new RuntimeException(String.format("Failed to denylist %s in %s.%s (see server logs)", key, keyspace, table));
        }
    }

    @Command(name = "remove", description = "Remove a partition key from the denylist")
    public static class RemoveCmd extends Denylist
    {
        @Arguments(usage = "<keyspace> <table> <partition_key>",
                   description = "Keyspace, table, and string form of the partition key to remove from the denylist",
                   required = true)
        private List<String> args = new ArrayList<>();

        @Override
        public void execute(NodeProbe probe)
        {
            if (args.size() != 3)
                throw new IllegalArgumentException("denylist remove requires exactly 3 arguments: <keyspace> <table> <partition_key>");

            final String keyspace = args.get(0);
            final String table = args.get(1);
            final String key = args.get(2);
            final PrintStream out = probe.output().out;

            if (probe.getSpProxy().removeDenylistKey(keyspace, table, key))
                out.printf("Removed %s from %s.%s denylist%n", key, keyspace, table);
            else
                throw new RuntimeException(String.format("Failed to remove %s from %s.%s denylist (see server logs)", key, keyspace, table));
        }
    }

    @Command(name = "refresh", description = "Force a reload of the partition denylist cache from system_distributed.partition_denylist")
    public static class RefreshCmd extends Denylist
    {
        @Override
        public void execute(NodeProbe probe)
        {
            final StorageProxyMBean sp = probe.getSpProxy();
            sp.loadPartitionDenylist();
            probe.output().out.printf("Denylist reloaded (attempts=%d, successes=%d)%n",
                                      sp.getPartitionDenylistLoadAttempts(),
                                      sp.getPartitionDenylistLoadSuccesses());
        }
    }

    @Command(name = "list", description = "List denylisted partition keys. With no arguments, lists all cached entries. " +
                                          "With <keyspace> <table>, lists entries for that table only.")
    public static class ListCmd extends Denylist
    {
        @Arguments(usage = "[<keyspace> <table>]",
                   description = "Optional keyspace and table to limit the listing")
        private List<String> args = new ArrayList<>();

        @Option(title = "format",
                name = { "-F", "--format" },
                description = "Output format (json, yaml)")
        private String outputFormat = "";

        @Override
        public void execute(NodeProbe probe)
        {
            if (!outputFormat.isEmpty() && !"json".equals(outputFormat) && !"yaml".equals(outputFormat))
                throw new IllegalArgumentException("arguments for -F are json,yaml only.");

            final PrintStream out = probe.output().out;
            final StorageProxyMBean sp = probe.getSpProxy();

            final Map<String, List<String>> data;
            final String scope;
            if (args.isEmpty())
            {
                data = new TreeMap<>(sp.getAllDenylistedKeys());
                scope = null;
            }
            else if (args.size() == 2)
            {
                final String keyspace = args.get(0);
                final String table = args.get(1);
                scope = keyspace + '.' + table;
                final List<String> keys = sp.getDenylistedKeys(keyspace, table);
                data = keys.isEmpty() ? Collections.emptyMap()
                                      : Collections.singletonMap(scope, keys);
            }
            else
            {
                throw new IllegalArgumentException("denylist list takes either no arguments or <keyspace> <table>");
            }

            final DenylistHolder holder = new DenylistHolder(data);
            if ("json".equals(outputFormat))
                new StatsPrinter.JsonPrinter<DenylistHolder>().print(holder, out);
            else if ("yaml".equals(outputFormat))
                new StatsPrinter.YamlPrinter<DenylistHolder>().print(holder, out);
            else
                printText(data, scope, out);
        }

        private static void printText(Map<String, List<String>> data, String scope, PrintStream out)
        {
            if (data.isEmpty())
            {
                if (scope == null)
                    out.println("No denylisted partition keys");
                else
                    out.printf("No denylisted partition keys for %s%n", scope);
                return;
            }
            for (Map.Entry<String, List<String>> entry : data.entrySet())
            {
                out.println(entry.getKey());
                for (String key : entry.getValue())
                    out.println("  " + key);
            }
        }
    }

    /**
     * Wraps the list result as a {@link StatsHolder} so it can be piped through
     * {@link StatsPrinter.JsonPrinter} / {@link StatsPrinter.YamlPrinter}.
     */
    static class DenylistHolder implements StatsHolder
    {
        private final Map<String, List<String>> data;

        DenylistHolder(Map<String, List<String>> data)
        {
            this.data = data;
        }

        @Override
        public Map<String, Object> convert2Map()
        {
            return new LinkedHashMap<>(data);
        }
    }
}
