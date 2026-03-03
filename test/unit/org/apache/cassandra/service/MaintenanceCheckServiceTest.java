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
package org.apache.cassandra.service;

import java.util.Map;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.junit.Test;

import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.MaintenanceCheckService.StopResult;
import org.apache.cassandra.service.MaintenanceCheckService.StopResult.ForKeyspace;
import org.apache.cassandra.service.MaintenanceCheckService.StopResult.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MaintenanceCheckServiceTest
{
    @Test
    public void testCompositeDataConversion() throws Exception
    {
        InetAddressAndPort target = InetAddressAndPort.getByName("127.0.0.1");
        StopResult result = new StopResult(target);
        result.add("ks_safe", new ForKeyspace(Status.SAFE, "Safe to stop (RF=3)"));
        result.add("ks_unsafe", new ForKeyspace(Status.UNSAFE, "Would break LOCAL_QUORUM for reads"));
        result.add("ks_lowrf", new ForKeyspace(Status.WARNING_LOW_RF, "Low replication factor (RF=2)"));
        result.add("ks_strategy", new ForKeyspace(Status.WARNING_STRATEGY, "Uses SimpleStrategy (RF=3)"));

        CompositeData cd = MaintenanceCheckCompositeData.from(result);

        assertFalse((Boolean) cd.get("verdict"));
        assertEquals(target.toString(), cd.get("target"));

        TabularData keyspaces = (TabularData) cd.get("keyspaces");
        assertEquals(4, keyspaces.size());

        CompositeData safe = getRow(keyspaces, "ks_safe");
        assertEquals("SAFE", safe.get("status"));
        assertNull(safe.get("consistency"));
        assertNull(safe.get("required"));
        assertNull(safe.get("alive"));
        assertNull(safe.get("blockedBy"));

        CompositeData unsafe = getRow(keyspaces, "ks_unsafe");
        assertEquals("UNSAFE", unsafe.get("status"));
    }

    @Test
    public void testCompositeDataEmpty() throws Exception
    {
        StopResult result = new StopResult(InetAddressAndPort.getByName("127.0.0.1"));
        CompositeData cd = MaintenanceCheckCompositeData.from(result);
        assertTrue((Boolean) cd.get("verdict"));
        TabularData keyspaces = (TabularData) cd.get("keyspaces");
        assertEquals(0, keyspaces.size());
    }

    @Test
    public void testVerdictReflectsUnsafe() throws Exception
    {
        InetAddressAndPort target = InetAddressAndPort.getByName("127.0.0.1");
        StopResult result = new StopResult(target);
        result.add("ks_safe", new ForKeyspace(Status.SAFE, "ok"));
        assertTrue("Should be safe with only SAFE keyspaces", result.verdict);

        result.add("ks_unsafe", new ForKeyspace(Status.UNSAFE, "broken"));
        assertFalse("Should be unsafe after adding UNSAFE keyspace", result.verdict);

        CompositeData cd = MaintenanceCheckCompositeData.from(result);
        assertFalse((Boolean) cd.get("verdict"));
    }

    @Test
    public void testDetailsWithBlockedBySerialization() throws Exception
    {
        InetAddressAndPort target = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort downNode = InetAddressAndPort.getByName("127.0.0.2");
        InetAddressAndPort leavingNode = InetAddressAndPort.getByName("127.0.0.3");

        UnavailableException cause = UnavailableException.create(ConsistencyLevel.LOCAL_QUORUM, 2, 0);
        Map<InetAddressAndPort, String> blockedBy = Map.of(downNode, "down", leavingNode, "leaving");

        StopResult result = new StopResult(target);
        result.add("ks1", new ForKeyspace(Status.UNSAFE, "Would break LOCAL_QUORUM for reads", cause, blockedBy));

        CompositeData cd = MaintenanceCheckCompositeData.from(result);
        TabularData keyspaces = (TabularData) cd.get("keyspaces");
        CompositeData row = getRow(keyspaces, "ks1");

        assertEquals("LOCAL_QUORUM", row.get("consistency"));
        assertEquals(2, row.get("required"));
        assertEquals(0, row.get("alive"));
        String blocked = (String) row.get("blockedBy");
        assertTrue(blocked.contains(downNode.toString()));
        assertTrue(blocked.contains("down"));
        assertTrue(blocked.contains(leavingNode.toString()));
        assertTrue(blocked.contains("leaving"));
    }

    private static CompositeData getRow(TabularData table, String keyspace)
    {
        return table.get(new Object[]{ keyspace });
    }
}
