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

public class ConfigCheckPrinter
{
    public static StatsPrinter<ConfigCheckHolder> from(String format)
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

    public static class DefaultPrinter implements StatsPrinter<ConfigCheckHolder>
    {
        @Override
        public void print(ConfigCheckHolder data, PrintStream out)
        {
            Map<String, Object> map = data.convert2Map();

            out.println("Loaded config hash: " + map.get("loaded_hash"));
            out.println("File config hash:   " + map.get("file_hash"));

            boolean changed = Boolean.TRUE.equals(map.get("changed"));
            if (!changed)
            {
                out.println("Config is unchanged.");
                return;
            }

            out.println("Config has been MODIFIED on disk since startup.");

            Object diffObj = map.get("diff");
            if (diffObj instanceof Map)
            {
                out.println();
                @SuppressWarnings("unchecked")
                Map<String, Map<String, String>> diff = (Map<String, Map<String, String>>) diffObj;
                for (Map.Entry<String, Map<String, String>> entry : diff.entrySet())
                {
                    Map<String, String> vals = entry.getValue();
                    out.println("  " + entry.getKey() + ": " + vals.get("loaded") + " -> " + vals.get("file"));
                }
            }
        }
    }
}
