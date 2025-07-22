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

package org.apache.cassandra.distributed.test;

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class HintsServiceConfigurationTest extends TestBaseImpl
{
    private static final int NUM_INSERTS = 1000;

    @Test
    public void testUpdateConfigurationWithThrottle() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                       .set("hinted_handoff_enabled", true)
                                                                       .set("max_hints_delivery_threads", "1")
                                                                       .set("hints_flush_period", "1s")
                                                                       .set("max_hints_size_per_host", "29MiB")
                                                                       .set("max_hints_file_size", "10MiB")
                                                                       .set("hinted_handoff_throttle_in_kb", "3"))
                                           .start(), 2))
        {
            final IInvokableInstance node1 = cluster.get(1);
            final IInvokableInstance node2 = cluster.get(2);

            String createTableStatement = format("CREATE TABLE %s.cf (k text PRIMARY KEY, c1 blob) " +
                                                 "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'} ", KEYSPACE);
            cluster.schemaChange(createTableStatement);

            UUID node2UUID = node2.callOnInstance((IIsolatedExecutor.SerializableCallable<UUID>) () -> StorageService.instance.getLocalHostUUID());

            // shutdown the second node in a blocking manner
            node2.shutdown().get();
            waitUntilNodeState(node1, node2UUID, false);

            // Generate the 1 KiB blob once, all zeros fine and compresses nicely
            byte[] oneKb = new byte[1024];

            // Write some data to generate hints
            for (int i = 0; i < NUM_INSERTS; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.cf (k, c1) VALUES (?, ?);"),
                                ONE, String.valueOf(i), oneKb);
            }

            // Wait for hints to be generated
            await().atMost(1, MINUTES)
                   .pollInterval(1, SECONDS)
                   .until(() -> node1.callOnInstance(() -> StorageMetrics.totalHints.getCount()) == NUM_INSERTS);

            // Get initial hints count
            long initialHintsCount = node1.callOnInstance(() -> StorageMetrics.totalHints.getCount());
            assertThat(initialHintsCount).isGreaterThan(0);

            // Start node2 back up to trigger hint delivery
            node2.startup();
            waitUntilNodeState(node1, node2UUID, true);

            // Update the throttle configuration, if the rate isnt changed it cant complete within timeout
            node1.runOnInstance(() -> DatabaseDescriptor.setHintedHandoffThrottleInKiB(1024));

            // Wait for hints to be delivered with the new throttle rate
            await().atMost(1, MINUTES)
                   .pollInterval(1, SECONDS)
                   .until(() -> node1.callOnInstance(() -> HintsServiceMetrics.hintsSucceeded.getCount() == NUM_INSERTS));

            // Verify all hints were delivered
            assertThat(node1.callOnInstance(() -> HintsServiceMetrics.hintsSucceeded.getCount())).isEqualTo(NUM_INSERTS);
        }
    }

    private void waitUntilNodeState(IInvokableInstance node, UUID node2UUID, boolean shouldBeOnline)
    {
        await().pollInterval(10, SECONDS)
               .timeout(1, MINUTES)
               .until(() -> node.appliesOnInstance((IIsolatedExecutor.SerializableBiFunction<UUID, Boolean, Boolean>) (secondNode, online) -> {
                   InetAddressAndPort address = StorageService.instance.getEndpointForHostId(secondNode);
                   return online == FailureDetector.instance.isAlive(address);
               }).apply(node2UUID, shouldBeOnline));
    }
} 