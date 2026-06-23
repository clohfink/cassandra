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

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertFalse;

/**
 * Exercises the Netflix "relaxed TRUNCATE" feature: a per-table opt-in via the
 * {@code netflix_relaxed_truncate} table option that lets TRUNCATE proceed as a
 * best-effort operation against the full token-owner set — succeeding (and being
 * applied) on whoever acks, and throwing a {@code TruncateException} naming the
 * replicas that didn't ack so the client retries until clean.
 *
 * Strict-path regression coverage lives in {@link FailingTruncationTest}.
 */
public class RelaxedTruncateTest extends TestBaseImpl
{
    @Test
    public void testRelaxedTruncateSkipsDownNode() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl_relaxed (id int primary key, v int) " +
                                 "WITH netflix_relaxed_truncate = true");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl_strict  (id int primary key, v int)");

            // Take node 3 down and wait for the ring on node 1 to see it as Down.
            ClusterUtils.stopUnchecked(cluster.get(3));
            ClusterUtils.awaitRingStatus(cluster.get(1), cluster.get(3), "Down");

            // Strict path: must throw unavailable-ish (regression guard).
            Assertions.assertThatThrownBy(() ->
                    cluster.coordinator(1).execute("TRUNCATE " + KEYSPACE + ".tbl_strict", ConsistencyLevel.ALL))
                      .hasMessageContaining("Cannot achieve consistency level");

            // Relaxed path with one replica missing: must throw a TruncateException naming the
            // missing endpoints, so the client knows to retry. The TRUNCATE has still been
            // applied on the two live replicas.
            Assertions.assertThatThrownBy(() ->
                    cluster.coordinator(1).execute("TRUNCATE " + KEYSPACE + ".tbl_relaxed", ConsistencyLevel.ALL))
                      .hasMessageContaining("Relaxed TRUNCATE")
                      .hasMessageContaining("incomplete")
                      .hasMessageContaining("Retry");

            // Coordinator log must record the missing-acks WARN.
            List<String> warns = cluster.get(1).logs()
                                               .grep("Relaxed TRUNCATE of .*tbl_relaxed.*did not complete on all replicas")
                                               .getResult();
            assertFalse("Expected a 'Relaxed TRUNCATE ... did not complete on all replicas' WARN on coordinator",
                        warns.isEmpty());
        }
    }

    @Test
    public void testRelaxedTruncateAllNodesUp() throws Exception
    {
        // With the option on AND all nodes up, the relaxed path must succeed cleanly (no throw)
        // AND actually delete the rows on every replica.
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int primary key, v int) " +
                                 "WITH netflix_relaxed_truncate = true");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, v) VALUES (?, ?)",
                                               ConsistencyLevel.ALL, i, i);

            cluster.coordinator(1).execute("TRUNCATE " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);

            Object[][] rows = cluster.coordinator(1).execute(
                "SELECT COUNT(*) FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            Assertions.assertThat((Long) rows[0][0])
                      .as("relaxed TRUNCATE must actually remove rows on every replica")
                      .isEqualTo(0L);
        }
    }

    @Test
    public void testJmxEmergencyRelaxedTruncate() throws Exception
    {
        // Emergency JMX path: bypasses the per-table option (option absent here).
        // Must still throw a TruncateException naming the down node, emit the WARN, and
        // actually delete data on the live replicas.
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id int primary key, v int)");
            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, v) VALUES (?, ?)",
                                               ConsistencyLevel.ALL, i, i);

            ClusterUtils.stopUnchecked(cluster.get(3));
            ClusterUtils.awaitRingStatus(cluster.get(1), cluster.get(3), "Down");

            Assertions.assertThatThrownBy(() -> cluster.get(1).runOnInstance(() -> {
                                                    try
                                                    {
                                                        StorageService.instance.truncateRelaxed(KEYSPACE, "tbl");
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        throw new RuntimeException(e);
                                                    }
                                                }))
                      .hasMessageContaining("Relaxed TRUNCATE")
                      .hasMessageContaining("incomplete");

            // Both audit log lines should be present on the coordinator: the operator-action
            // record from StorageService and the per-attempt WARN from StorageProxy.
            assertFalse("Expected the operator-initiated WARN on the coordinator",
                        cluster.get(1).logs()
                               .grep("Operator-initiated emergency relaxed TRUNCATE of .*tbl")
                               .getResult().isEmpty());
            assertFalse("Expected the missing-acks WARN on the coordinator",
                        cluster.get(1).logs()
                               .grep("Relaxed TRUNCATE of .*tbl.*did not complete on all replicas")
                               .getResult().isEmpty());

            // Data must be gone on the two live replicas (CL.TWO covers them both).
            Object[][] rows = cluster.coordinator(1).execute(
                "SELECT COUNT(*) FROM " + KEYSPACE + ".tbl", ConsistencyLevel.TWO);
            Assertions.assertThat((Long) rows[0][0])
                      .as("emergency relaxed TRUNCATE must remove rows on live replicas")
                      .isEqualTo(0L);
        }
    }
}
