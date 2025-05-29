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

package com.netflix.cassandra.virtual;

import com.netflix.cassandra.db.virtual.ClusterPartitionCount;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.concurrent.Future;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ClusterPartitionCountTest
{
    private static final String RACK1 = "rack1";
    private static final String RACK2 = "rack2";

    @Mock
    private IEndpointSnitch snitch;

    @Mock
    private Future<Message<ReadResponse>> future1;

    @Mock
    private Future<Message<ReadResponse>> future2;

    @Mock
    private Future<Message<ReadResponse>> future3;

    private InetAddressAndPort endpoint1;
    private InetAddressAndPort endpoint2;
    private InetAddressAndPort endpoint3;

    private ClusterPartitionCount clusterPartitionCount;
    private Map<InetAddressAndPort, Future<Message<ReadResponse>>> results;
    private Map<String, Set<InetAddressAndPort>> rackNodes;
    private Map<String, Set<InetAddressAndPort>> rackResponses;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup() throws Exception
    {
        // Create test endpoints
        endpoint1 = InetAddressAndPort.getByAddress(InetAddress.getByName("127.0.0.1"));
        endpoint2 = InetAddressAndPort.getByAddress(InetAddress.getByName("127.0.0.2"));
        endpoint3 = InetAddressAndPort.getByAddress(InetAddress.getByName("127.0.0.3"));

        // Setup snitch mock
        when(snitch.getRack(endpoint1)).thenReturn(RACK1);
        when(snitch.getRack(endpoint2)).thenReturn(RACK1);

        // Initialize test objects
        clusterPartitionCount = new ClusterPartitionCount("test_keyspace");
        results = new HashMap<>();
        rackNodes = new HashMap<>();
        rackResponses = new HashMap<>();

        // Setup rack nodes
        Set<InetAddressAndPort> rack1Nodes = new HashSet<>();
        rack1Nodes.add(endpoint1);
        rack1Nodes.add(endpoint2);
        rackNodes.put(RACK1, rack1Nodes);

        Set<InetAddressAndPort> rack2Nodes = new HashSet<>();
        rack2Nodes.add(endpoint3);
        rackNodes.put(RACK2, rack2Nodes);

        // Add futures to results
        results.put(endpoint1, future1);
        results.put(endpoint2, future2);
        results.put(endpoint3, future3);
    }

    @Test
    public void testWaitForFullRack_Success() throws Exception
    {
        when(future1.isDone()).thenReturn(true);
        when(future2.isDone()).thenReturn(true);

        boolean result = clusterPartitionCount.waitForFullRack(results, rackNodes, rackResponses, snitch);

        assertTrue("Should return true when a full rack is found", result);
        assertEquals("Rack1 should have 2 responses", 2, rackResponses.get(RACK1).size());
        assertTrue("Rack1 responses should include endpoint1", rackResponses.get(RACK1).contains(endpoint1));
        assertTrue("Rack1 responses should include endpoint2", rackResponses.get(RACK1).contains(endpoint2));
    }

    @Test
    public void testWaitForFullRack_Timeout() throws Exception
    {
        when(future1.isDone()).thenReturn(false);
        when(future2.isDone()).thenReturn(false);
        when(future3.isDone()).thenReturn(false);

        boolean result = clusterPartitionCount.waitForFullRack(results, rackNodes, rackResponses, snitch);

        assertFalse("Should return false when timeout occurs", result);
        assertTrue("Rack responses should be empty", rackResponses.isEmpty());
    }

    @Test
    public void testWaitForFullRack_PartialRack() throws Exception
    {
        when(future1.isDone()).thenReturn(true);
        when(future2.isDone()).thenReturn(false);
        when(future3.isDone()).thenReturn(false);

        boolean result = clusterPartitionCount.waitForFullRack(results, rackNodes, rackResponses, snitch);

        assertFalse("Should return false when only partial rack is found", result);
        assertEquals("Rack1 should have 1 response", 1, rackResponses.get(RACK1).size());
        assertTrue("Rack1 responses should include endpoint1", rackResponses.get(RACK1).contains(endpoint1));
    }
}
