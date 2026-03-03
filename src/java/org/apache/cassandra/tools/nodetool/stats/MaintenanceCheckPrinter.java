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
import java.util.Map;

import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

public class MaintenanceCheckPrinter
{
    public static StatsPrinter<MaintenanceCheckHolder> from(String format)
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

    public static class DefaultPrinter implements StatsPrinter<MaintenanceCheckHolder>
    {
        @Override
        public void print(MaintenanceCheckHolder data, PrintStream out)
        {
            Map<String, Object> map = data.convert2Map();

            Object verdict = map.get("verdict");
            out.println("Verdict: " + (Boolean.TRUE.equals(verdict) ? "SAFE" : "UNSAFE"));
            out.println("Target: " + map.get("target"));
            out.println();

            Object keyspacesObj = map.get("keyspaces");
            if (!(keyspacesObj instanceof Map))
                return;

            @SuppressWarnings("unchecked")
            Map<String, Object> keyspaces = (Map<String, Object>) keyspacesObj;

            TableBuilder table = new TableBuilder();
            table.add("Keyspace", "Status", "Message", "Consistency", "Required", "Alive", "Blocked by");

            for (Map.Entry<String, Object> entry : keyspaces.entrySet())
            {
                if (!(entry.getValue() instanceof Map))
                    continue;

                @SuppressWarnings("unchecked")
                Map<String, Object> ks = (Map<String, Object>) entry.getValue();
                table.add(entry.getKey(),
                          str(ks.get("status")),
                          str(ks.get("message")),
                          str(ks.get("consistency")),
                          str(ks.get("required")),
                          str(ks.get("alive")),
                          str(ks.get("blockedBy")));
            }
            table.printTo(out);
        }

        private static String str(Object value)
        {
            return value != null ? String.valueOf(value) : "";
        }
    }
}
