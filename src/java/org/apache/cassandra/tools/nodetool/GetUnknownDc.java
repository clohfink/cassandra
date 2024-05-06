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

import io.airlift.airline.Command;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import java.net.UnknownHostException;
import java.util.Set;
import java.util.stream.Collectors;

@Command(name = "getunknowndc", description = "Get the unknown datacenter for a node")
public class GetUnknownDc extends NodeTool.NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
            Set<String> dcs = probe.getTokenToEndpointMap(true).values().stream()
                    .map(endpoint -> {
                        try {
                            return epSnitchInfo.getDatacenter(endpoint);
                        } catch (UnknownHostException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toSet());

            dcs.stream()
                    .filter(dc -> dc.equals("UNKNOWN_DC"))
                    .forEach(dc -> probe.output().out.println("Datacenter: " + dc));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error during probing", e);
        }
    }
}