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

package org.apache.cassandra.locator;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.LocateService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

public class GossipingLocatorSnitch extends GossipingPropertyFileSnitch
{
    private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);
    private static final NoSpamLogger nospam1m = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public String getDatacenter(InetAddressAndPort endpoint) {
        String dc = super.getDatacenter(endpoint);
        if (!dc.equals(GossipingPropertyFileSnitch.DEFAULT_DC))
            return dc;

        String locatedDc = LocateService.instance.getDatacenter(endpoint);
        if (locatedDc != null)
            return locatedDc;

        handleUnknownDatacenter(endpoint);
        return GossipingPropertyFileSnitch.DEFAULT_DC;
    }

    /**
     * Its unsafe to use incorrect DC/Rack information when gossip missing info
     * see: <a href="https://netflix.atlassian.net/browse/ODS-136">ODS-136</a>
     * @param endpoint
     */
    private void handleUnknownDatacenter(InetAddressAndPort endpoint)
    {
        String clusterName = DatabaseDescriptor.getClusterName();
        if (DatabaseDescriptor.getDieOnUnknownGossipState())
        {
            String errorMessage = String.format("Unknown DC for cluster: %s, endpoint: %s, not in gossip. " +
                                                "Disable this check with die_on_unknown_gossip_state in " +
                                                "cassandra.yaml. Shutting down server.", clusterName, endpoint);
            Throwable t = new RuntimeException(errorMessage);
            logger.error(errorMessage, t);
            JVMStabilityInspector.killCurrentJVM(t, true);
        }
        else
        {
            nospam1m.warn("Unknown DC for cluster: {}, endpoint: {}, defaulting to {}",
                          clusterName, endpoint, GossipingPropertyFileSnitch.DEFAULT_DC);
        }
    }

    public String getRack(InetAddressAndPort endpoint)
    {
        String rack = super.getRack(endpoint);
        if (!rack.equals(GossipingPropertyFileSnitch.DEFAULT_RACK))
            return rack;

        String locatedRack = LocateService.instance.getRack(endpoint);
        if (locatedRack != null)
            return locatedRack;

        return GossipingPropertyFileSnitch.DEFAULT_RACK;
    }
}
