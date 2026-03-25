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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import javax.management.openmbean.CompositeData;

import org.yaml.snakeyaml.Yaml;

public class ConfigCheckHolder implements StatsHolder
{
    private final CompositeData data;
    private final boolean showDiff;

    public ConfigCheckHolder(CompositeData data, boolean showDiff)
    {
        this.data = data;
        this.showDiff = showDiff;
    }

    @Override
    public Map<String, Object> convert2Map()
    {
        Map<String, Object> result = new LinkedHashMap<>();
        boolean changed = (Boolean) data.get("changed");
        result.put("changed", changed);
        result.put("loaded_hash", data.get("loaded_hash"));
        result.put("file_hash", data.get("file_hash"));

        if (changed && showDiff)
        {
            String loadedConfig = (String) data.get("loaded_config");
            String fileConfig = (String) data.get("file_config");
            if (loadedConfig != null && fileConfig != null)
                result.put("diff", computeDiff(loadedConfig, fileConfig));
        }

        return result;
    }

    /**
     * Parse both YAML configs into flat dot-notation maps and return only the keys that differ.
     * Each entry maps a dot-notation key to {"loaded": oldValue, "file": newValue}.
     * Missing keys on either side use null.
     */
    static Map<String, Map<String, String>> computeDiff(String loaded, String file)
    {
        Map<String, String> loadedFlat = flatten(new Yaml().load(loaded));
        Map<String, String> fileFlat = flatten(new Yaml().load(file));

        TreeSet<String> allKeys = new TreeSet<>();
        allKeys.addAll(loadedFlat.keySet());
        allKeys.addAll(fileFlat.keySet());

        Map<String, Map<String, String>> diff = new LinkedHashMap<>();
        for (String key : allKeys)
        {
            String loadedVal = loadedFlat.get(key);
            String fileVal = fileFlat.get(key);
            if (loadedVal == null && fileVal == null)
                continue;
            if (loadedVal != null && loadedVal.equals(fileVal))
                continue;

            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("loaded", loadedVal);
            entry.put("file", fileVal);
            diff.put(key, entry);
        }
        return diff;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> flatten(Object yaml)
    {
        Map<String, String> result = new LinkedHashMap<>();
        if (yaml instanceof Map)
            flattenMap("", (Map<String, Object>) yaml, result);
        return result;
    }

    @SuppressWarnings("unchecked")
    private static void flattenMap(String prefix, Map<String, Object> map, Map<String, String> result)
    {
        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + '.' + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map)
                flattenMap(key, (Map<String, Object>) value, result);
            else
                result.put(key, value == null ? null : String.valueOf(value));
        }
    }
}
