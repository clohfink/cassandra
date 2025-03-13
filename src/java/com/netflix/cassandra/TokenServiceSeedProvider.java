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
package com.netflix.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SeedProvider;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TokenServiceSeedProvider is a custom implementation of the Cassandra SeedProvider interface.
 * It retrieves seed nodes from a remote token service via a REST API.
 */
public class TokenServiceSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(TokenServiceSeedProvider.class);
    // Regular expression pattern for validating IPv4 addresses
    private static final Pattern IPV4_PATTERN =
    Pattern.compile("^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$");
    // Regular expression pattern for validating IPv6 addresses
    private static final Pattern IPV6_PATTERN =
    Pattern.compile("^(::)?[0-9a-fA-F]{1,4}(::?[0-9a-fA-F]{1,4}){1,7}(::)?$");

    private final TokenService tokenService;

    public TokenServiceSeedProvider()
    {
        this(new TokenService());
    }

    public TokenServiceSeedProvider(TokenService tokenService)
    {
        this.tokenService = tokenService;
    }

    /**
     * Pulls all the instances from token service and picks the seeds based on the following criteria:
     *
     * <ul>
     *   <li>Nodes with an instance ID of "new_slot" are skipped.</li>
     *   <li>If auto-bootstrap is enabled, the current instance ID is also skipped.</li>
     *   <li>Only first node per availability zone is added.</li>
     *   <li>The node must have a valid IP address.</li>
     * </ul>
     *
     * @return a list of valid {@code InetAddressAndPort} objects representing the seed nodes.
     * @throws RuntimeException if an error occurs during TokenService fetch, JSON processing or IP address resolution.
     */
    @Override
    public List<InetAddressAndPort> getSeeds()
    {
        try
        {
            // Fetch all instances from the token service
            List<NetflixInstance> instances = tokenService.getInstances();

            // Use the abstracted filterSeeds method
            return filterSeeds(instances, this.tokenService.instanceId, DatabaseDescriptor.isAutoBootstrap());
        }
        catch (Exception exception)
        {
            throw new RuntimeException("Failed to load seed data", exception);
        }
    }

    @VisibleForTesting
    public static List<InetAddressAndPort> filterSeeds(List<NetflixInstance> instances,
                                                       @Nullable String currentInstanceId,
                                                       boolean isAutoBootstrap)
    throws UnknownHostException
    {
        List<InetAddressAndPort> seeds = new ArrayList<>();
        Set<String> availabilityZones = new HashSet<>();

        for (NetflixInstance node : instances)
        {
            // Skip if the instance ID is "new_slot" or matches the current instance ID (unless autoBootstrap is false)
            if ("new_slot".equals(node.getInstanceId()) ||
                (isAutoBootstrap && node.getInstanceId().equals(currentInstanceId)))
                continue;

            // 1 seed per availability zone (AZ)
            String availabilityZone = node.getAvailabilityZone();
            if (availabilityZones.contains(availabilityZone))
                continue;

            // Verify if the host IP is valid
            String hostIP = node.getHostIP();
            if (isValidIP(hostIP))
            {
                logger.info("Adding seed node with IP: {} from availability zone: {}", hostIP, availabilityZone);
                availabilityZones.add(availabilityZone);
                seeds.add(InetAddressAndPort.getByName(hostIP));
            }
            else
            {
                logger.warn("Invalid IP address: {}, skipping", hostIP);
            }
        }
        return seeds;
    }

    /**
     * Validates if the given IP address is either a valid IPv4 or IPv6 address.
     *
     * @param ip the IP address to validate.
     * @return true if the IP address is valid and not "0.0.0.0", false otherwise.
     */
    public static boolean isValidIP(String ip)
    {
        return (IPV4_PATTERN.matcher(ip).matches() || IPV6_PATTERN.matcher(ip).matches()) && !"0.0.0.0".equals(ip);
    }
}
