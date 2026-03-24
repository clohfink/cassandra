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
import java.util.List;
import java.util.Map;
import javax.management.openmbean.CompositeData;

import org.apache.cassandra.service.RepairStatusCompositeData;

public class RepairStatusHolder implements StatsHolder
{
    private final Map<String, Object> data;

    public RepairStatusHolder(CompositeData cd, List<String> sections)
    {
        Map<String, Object> parsed = RepairStatusCompositeData.fromCompositeData(cd).toMap();

        if (sections != null && !sections.isEmpty())
        {
            Map<String, Object> filtered = new LinkedHashMap<>();
            for (String section : sections)
            {
                if (parsed.containsKey(section))
                    filtered.put(section, parsed.get(section));
            }
            this.data = filtered;
        }
        else
        {
            this.data = parsed;
        }
    }

    @Override
    public Map<String, Object> convert2Map()
    {
        return data;
    }
}
