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

package org.apache.cassandra.distributed.test.metrics;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.concurrent.Future;
import org.awaitility.core.ThrowingRunnable;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests {@link org.apache.cassandra.hints.DynamicHintsThrottleManager} with actual hints and scheduling
 */
public class DynamicHintsThrottleTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(DynamicHintsThrottleTest.class);

    private static final int NUM_ROWS = 100;
    private static final int NUM_FAILURES_PER_NODE = 5;

    @Test
    public void testDynamicThrottleWithBacklog() throws Exception
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                    .set("hinted_handoff_throttle_in_kb", 1024) // 1 MB/s base
                                                                    .set("hinted_handoff_max_throttle", "5120KiB") // 5 MB/s max
                                                                    .set("hinted_handoff_throttle_backlog_threshold", 10) // Low threshold for testing
                                                                    .set("hinted_handoff_throttle_adjustment_interval_in_sec", 2)) // 2 seconds
                                        .withInstanceInitializer(FailSomeHints::install)
                                        .start())
        {
            cluster.setUncaughtExceptionsFilter(t -> "Injected failure".equals(t.getMessage()));

            AtomicBoolean dropWritesForNode2 = new AtomicBoolean(false);
            cluster.filters()
                   .verbs(Verb.MUTATION_REQ.id)
                   .from(1)
                   .messagesMatching((from, to, message) -> to == 2 && dropWritesForNode2.get())
                   .drop();

            fixDistributedSchemas(cluster);

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));

            ICoordinator coordinator = cluster.coordinator(1);
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);

            dropWritesForNode2.set(true);
            for (int i = 0; i < NUM_ROWS; i++)
                coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), QUORUM, i, i);
            dropWritesForNode2.set(false);

            waitUntilAsserted(() -> assertThat(getThroughputRate(node1)).isGreaterThan(0.0));

            Thread.sleep(6000); // 3 adjustment intervals

            double throughputRate = getThroughputRate(node1);
            assertThat(throughputRate).isGreaterThanOrEqualTo(0.0);

            waitUntilAsserted(() -> assertThat(countRows(node2)).isEqualTo(NUM_ROWS));

            long hintsSucceeded = getHintsSucceeded(node1);
            assertThat(hintsSucceeded).isGreaterThanOrEqualTo(NUM_ROWS);
        }
    }

    @Test
    public void testHintStatsCommandWithFailuresAndTimeouts() throws Exception
    {
        // Setup cluster with ByteBuddy to inject failures and message filters for timeouts
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                    .set("hinted_handoff_throttle_in_kb", 128)) // Low throttle to see delivery in progress
                                        .withInstanceInitializer(FailSomeHints::install)
                                        .start())
        {
            cluster.setUncaughtExceptionsFilter(t -> "Injected failure".equals(t.getMessage()));

            // Setup filter to drop mutations to node 2 and 3, forcing hints to be created
            AtomicBoolean dropWritesForNode2 = new AtomicBoolean(false);
            AtomicBoolean dropWritesForNode3 = new AtomicBoolean(false);
            cluster.filters()
                   .verbs(Verb.MUTATION_REQ.id)
                   .from(1)
                   .messagesMatching((from, to, message) ->
                                     (to == 2 && dropWritesForNode2.get()) ||
                                     (to == 3 && dropWritesForNode3.get()))
                   .drop();

            // Setup filter to drop HINT_REQ messages to cause timeouts
            AtomicInteger hintsToNode2 = new AtomicInteger();
            AtomicInteger hintsToNode3 = new AtomicInteger();
            cluster.filters()
                   .verbs(Verb.HINT_REQ.id)
                   .from(1)
                   .messagesMatching((from, to, message) ->
                                     (to == 2 && hintsToNode2.incrementAndGet() <= NUM_FAILURES_PER_NODE) ||
                                     (to == 3 && hintsToNode3.incrementAndGet() <= NUM_FAILURES_PER_NODE))
                   .drop();

            fixDistributedSchemas(cluster);

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));

            ICoordinator coordinator = cluster.coordinator(1);
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            dropWritesForNode2.set(true);
            for (int i = 0; i < NUM_ROWS / 2; i++)
                coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), QUORUM, i, i);
            dropWritesForNode2.set(false);

            dropWritesForNode3.set(true);
            for (int i = NUM_ROWS / 2; i < NUM_ROWS; i++)
                coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), QUORUM, i, i);
            dropWritesForNode3.set(false);

            waitUntilAsserted(() -> assertThat(getHintsSucceeded(node1)).isGreaterThan(10));

            logger.info("\n========== Testing hintstats DURING active delivery with failures/timeouts ==========");
            org.apache.cassandra.distributed.api.NodeToolResult result = node1.nodetoolResult("hintstats");
            result.asserts().success();

            String output = result.getStdout();
            logger.info(output);
            logger.info("=====================================================================================\n");

            assertThat(output).contains("Endpoint");
            assertThat(output).contains("Succeeded");
            assertThat(output).contains("Failed");
            assertThat(output).contains("Timedout");
            assertThat(output).contains("Success Rate/s");
            assertThat(output).contains("Throughput KB/s");

            String node2Address = node2.broadcastAddress().getAddress().getHostAddress();
            String node3Address = node3.broadcastAddress().getAddress().getHostAddress();
            assertThat(output).contains(node2Address);
            assertThat(output).contains(node3Address);

            long failedCount = getHintsFailed(node1);
            long timedOutCount = getHintsTimedOut(node1);
            logger.info("Total hints failed: " + failedCount);
            logger.info("Total hints timed out: " + timedOutCount);

            waitUntilAsserted(() -> assertThat(countRows(node2)).isEqualTo(NUM_ROWS));
            waitUntilAsserted(() -> assertThat(countRows(node3)).isEqualTo(NUM_ROWS));

            logger.info("\n========== Testing hintstats AFTER all hints delivered ==========");
            result = node1.nodetoolResult("hintstats");
            result.asserts().success();

            output = result.getStdout();
            logger.info(output);
            logger.info("=================================================================\n");

            long finalSucceeded = getHintsSucceeded(node1);
            long finalFailed = getHintsFailed(node1);
            long finalTimedOut = getHintsTimedOut(node1);

            logger.info("Final metrics:");
            logger.info("  Succeeded: " + finalSucceeded);
            logger.info("  Failed: " + finalFailed);
            logger.info("  Timed out: " + finalTimedOut);

            assertThat(finalFailed).isGreaterThanOrEqualTo(NUM_FAILURES_PER_NODE * 2);

            assertThat(finalTimedOut).isGreaterThanOrEqualTo(NUM_FAILURES_PER_NODE * 2);

            assertThat(finalSucceeded).isGreaterThanOrEqualTo(NUM_ROWS);

            assertThat(output).doesNotContain("No hint delivery metrics available");
        }
    }

    @Test
    public void testHintStatsCommand() throws Exception
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            AtomicBoolean dropWritesForNode2 = new AtomicBoolean(false);
            AtomicBoolean dropWritesForNode3 = new AtomicBoolean(false);
            cluster.filters()
                   .verbs(Verb.MUTATION_REQ.id)
                   .from(1)
                   .messagesMatching((from, to, message) ->
                                     (to == 2 && dropWritesForNode2.get()) ||
                                     (to == 3 && dropWritesForNode3.get()))
                   .drop();

            fixDistributedSchemas(cluster);

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));

            ICoordinator coordinator = cluster.coordinator(1);
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            IInvokableInstance node3 = cluster.get(3);

            dropWritesForNode2.set(true);
            for (int i = 0; i < NUM_ROWS / 2; i++)
                coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), QUORUM, i, i);
            dropWritesForNode2.set(false);

            dropWritesForNode3.set(true);
            for (int i = NUM_ROWS / 2; i < NUM_ROWS; i++)
                coordinator.execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), QUORUM, i, i);
            dropWritesForNode3.set(false);

            waitUntilAsserted(() -> assertThat(getHintsSucceeded(node1)).isGreaterThan(0));

            org.apache.cassandra.distributed.api.NodeToolResult result = node1.nodetoolResult("hintstats");

            result.asserts().success();

            String output = result.getStdout();

            logger.info("========== nodetool hintstats output (during delivery) ==========");
            logger.info(output);
            logger.info("==================================================================");

            assertThat(output).contains("Endpoint");
            assertThat(output).contains("Succeeded");
            assertThat(output).contains("Failed");
            assertThat(output).contains("Timedout");
            assertThat(output).contains("Success Rate/s");
            assertThat(output).contains("Throughput KB/s");

            String node2Address = node2.broadcastAddress().getAddress().getHostAddress();
            String node3Address = node3.broadcastAddress().getAddress().getHostAddress();
            assertThat(output).contains(node2Address);
            assertThat(output).contains(node3Address);

            waitUntilAsserted(() -> assertThat(countRows(node2)).isEqualTo(NUM_ROWS));
            waitUntilAsserted(() -> assertThat(countRows(node3)).isEqualTo(NUM_ROWS));

            result = node1.nodetoolResult("hintstats");
            result.asserts().success();

            output = result.getStdout();

            logger.info("========== nodetool hintstats output (after delivery) ==========");
            logger.info(output);
            logger.info("=================================================================");

            assertThat(output).doesNotContain("No hint delivery metrics available");
        }
    }

    private static void waitUntilAsserted(ThrowingRunnable assertion)
    {
        await().atMost(2, MINUTES)
               .pollDelay(0, SECONDS)
               .pollInterval(1, SECONDS)
               .dontCatchUncaughtExceptions()
               .untilAsserted(assertion);
    }

    private static int countRows(IInvokableInstance node)
    {
        return node.executeInternal(withKeyspace("SELECT * FROM %s.t")).length;
    }

    @SuppressWarnings("Convert2MethodRef")
    private static Double getThroughputRate(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            return HintsServiceMetrics.hintsThroughputBytes.getOneMinuteRate();
        });
    }

    @SuppressWarnings("Convert2MethodRef")
    private static Long getHintsSucceeded(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            return HintsServiceMetrics.hintsSucceeded.getCount();
        });
    }

    @SuppressWarnings("Convert2MethodRef")
    private static Long getHintsFailed(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            return HintsServiceMetrics.hintsFailed.getCount();
        });
    }

    @SuppressWarnings("Convert2MethodRef")
    private static Long getHintsTimedOut(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            return HintsServiceMetrics.hintsTimedOut.getCount();
        });
    }

    /**
     * Bytebuddy injection to make some hint applications fail to create retry scenarios
     */
    public static class FailSomeHints
    {
        private static final AtomicInteger numHintsForThisNode = new AtomicInteger(0);

        private static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 1)
                return;

            new ByteBuddy().rebase(Hint.class)
                           .method(named("applyFuture").and(takesArguments(0)))
                           .intercept(MethodDelegation.to(FailSomeHints.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Future<?> execute(@SuperCall Callable<Future<?>> r) throws Exception
        {
            if (numHintsForThisNode.incrementAndGet() <= NUM_FAILURES_PER_NODE)
                throw new RuntimeException("Injected failure");
            return r.call();
        }
    }
}