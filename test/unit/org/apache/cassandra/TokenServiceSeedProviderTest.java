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
package org.apache.cassandra;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.cassandra.NetflixInstance;
import com.netflix.cassandra.TokenService;
import com.netflix.cassandra.TokenServiceSeedProvider;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;

import java.util.ArrayList;
import java.util.List;

import static com.netflix.cassandra.TokenServiceSeedProvider.filterSeeds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TokenServiceSeedProviderTest
{
    private TokenServiceSeedProvider seedProvider;
    private List<NetflixInstance> instances;
    private String currentInstanceId = "i-123456789";
    private boolean isAutoBootstrap = true;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        seedProvider = new TokenServiceSeedProvider(new TokenService("app", "us-east-1", "env", "i-123456"));
        instances = new ArrayList<>();
        instances.add(new NetflixInstance(1728398107613L, 1728397893251L, "cass_perf_cl_large",
                                          "i-123456789", "us-east-1a", "-7173733804634027806", "us-east-1",
                                          -1670265060, "100.107.12.161", "ip-100-107-12-161.ec2.internal", null));
        instances.add(new NetflixInstance(1728398107614L, 1728397893252L, "cass_perf_cl_large",
                                          "i-987654321", "us-east-1b", "-7173733804634027807", "us-east-1",
                                          -1670265061, "100.107.12.162", "ip-100-107-12-162.ec2.internal", null));
        instances.add(new NetflixInstance(1728398107615L, 1728397893253L, "cass_perf_cl_large",
                                          "new_slot", "us-east-1c", "-7173733804634027808", "us-east-1",
                                          -1670265062, "100.107.12.163", "ip-100-107-12-163.ec2.internal", null));
    }

    @Test
    public void testIsValidIP()
    {
        // Test valid IPv4 addresses
        assertTrue(seedProvider.isValidIP("192.168.1.1"));
        assertTrue(seedProvider.isValidIP("8.8.8.8"));

        // Test invalid IPv4 addresses
        assertFalse(seedProvider.isValidIP("0.0.0.0"));
        assertFalse(seedProvider.isValidIP("9999.999.999.999"));
        assertFalse(seedProvider.isValidIP("null"));

        // Test valid IPv6 addresses
        assertTrue(seedProvider.isValidIP("2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
        assertTrue(seedProvider.isValidIP("fe80::1"));

        // Test invalid IPv6 addresses
        assertFalse(seedProvider.isValidIP("null"));
        assertFalse(seedProvider.isValidIP("12345::abcd"));
    }

    @Test
    public void testFilterSeeds_excludesCurrentInstance() throws Exception
    {
        // Test when auto-bootstrap is enabled and the current instance should be excluded
        List<InetAddressAndPort> seeds = filterSeeds(instances, currentInstanceId, isAutoBootstrap);

        // Ensure the current instance (i-123456789) is excluded
        assertEquals(1, seeds.size());
        assertEquals("100.107.12.162", seeds.get(0).getAddress().getHostAddress());
    }

    @Test
    public void testFilterSeeds_includesCurrentInstanceWhenNoAutoBootstrap() throws Exception
    {
        // Test when auto-bootstrap is disabled, and current instance should be included
        List<InetAddressAndPort> seeds = filterSeeds(instances, currentInstanceId, false);

        // Ensure the current instance is included
        assertEquals(2, seeds.size());
        seeds = filterSeeds(instances, currentInstanceId, false);

        // Ensure the current instance is included
        assertEquals(2, seeds.size());
    }

    @Test
    public void testFilterSeeds_skipsNewSlotInstance() throws Exception
    {
        // Test to ensure that "new_slot" instance is skipped
        List<InetAddressAndPort> seeds = filterSeeds(instances, currentInstanceId, isAutoBootstrap);

        // Ensure "new_slot" instance is skipped
        assertEquals(1, seeds.size());
        assertNotEquals("100.107.12.163", seeds.get(0).getAddress().getHostAddress());
    }

    @Test
    public void testFilterSeeds_validIPOnly() throws Exception
    {
        // Mock an instance with an invalid IP
        instances.add(new NetflixInstance(1728398107616L, 1728397893254L, "cass_perf_cl_large",
                                          "i-invalid-ip", "us-east-1d", "-7173733804634027809", "us-east-1",
                                          -1670265063, "invalid-ip", "ip-invalid-ip", null));

        // Test to ensure that only valid IPs are included
        List<InetAddressAndPort> seeds = filterSeeds(instances, currentInstanceId, isAutoBootstrap);

        // Ensure only valid IPs are included
        assertEquals(1, seeds.size());
        assertEquals("100.107.12.162", seeds.get(0).getAddress().getHostAddress());
    }

    @Test
    public void testFilterSeeds_onePerAvailabilityZone() throws Exception
    {
        // Add another instance from the same AZ "us-east-1a"
        instances.add(new NetflixInstance(1728398107617L, 1728397893255L, "cass_perf_cl_large",
                                          "i-111111111", "us-east-1a", "-7173733804634027810", "us-east-1",
                                          -1670265064, "100.107.12.164", "ip-100-107-12-164.ec2.internal", null));
        instances.add(new NetflixInstance(1728398107627L, 1728397893295L, "cass_perf_cl_large",
                                          "i-111111111", "us-east-1a", "-7173733804634027810", "us-east-1",
                                          -1670265065, "100.107.12.165", "ip-100-107-12-165.ec2.internal", null));

        // Test that only one instance per availability zone is selected
        List<InetAddressAndPort> seeds = filterSeeds(instances, currentInstanceId, isAutoBootstrap);


        // There should be 2 seed from "us-east-1a" and 1 from "us-east-1b"
        assertEquals(2, seeds.size());
    }
}
