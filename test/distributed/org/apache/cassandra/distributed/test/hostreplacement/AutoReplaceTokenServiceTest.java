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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.dht.StreamStateStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.Shared;
import org.assertj.core.api.Assertions;

import static java.util.concurrent.TimeUnit.MINUTES;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS;
import static org.apache.cassandra.config.CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK;
import static org.apache.cassandra.distributed.shared.ClusterUtils.assertRingIs;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitGossipStatus;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingHealthy;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingStatus;
import static org.apache.cassandra.distributed.shared.ClusterUtils.gossipInfo;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;

/**
 * Coverage for the Netflix {@code auto_replace} host-replacement path, where a freshly started node pulls
 * its token from the token service ({@code initial_token} is null) and, if that token is already owned by a
 * dead node, replaces it.
 *
 * The headline behaviours, all of which the upstream documentation describes incorrectly for this fork:
 *
 * <ol>
 *   <li>The replacing node advertises {@code STATUS_BOOTSTRAPPING_REPLACE} ("BOOT_REPLACE"), not
 *       {@code HIBERNATE}. Hibernate is only used when replacing the <i>same</i> address; auto_replace
 *       always assigns a brand new node a different address than the node it replaces.</li>
 *   <li>{@code BOOT_REPLACE} is in {@code Gossiper.SILENT_SHUTDOWN_STATES} but not {@code DEAD_STATES}, so
 *       peers keep treating the replacing node as alive (a pending replica): reads stay available and
 *       writes are forwarded to it, unlike HIBERNATE where writes are not forwarded.</li>
 * </ol>
 *
 * The remaining tests cover the guards and convergence properties around that path: auto_replace refuses to
 * replace a node that is still alive, leaves a non-conflicting (token service assigned an unowned token)
 * startup as an ordinary bootstrap, and converges on a peer that never saw the intermediate replacement
 * gossip.
 *
 * {@code auto_replace} is enabled through the yaml config setting (per instance), not the
 * {@code cassandra.auto_replace} system property, so the tests exercise the same wiring a managed/DGW
 * deployment uses.
 */
public class AutoReplaceTokenServiceTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AutoReplaceTokenServiceTest.class);

    private static final int SEED_NUM = 1;
    private static final int NODE_TO_REPLACE_NUM = 2;
    private static final int PEER_NUM = 3;
    private static final int REPLACEMENT_NUM = 4; // the node added to replace #2

    // set by production code (BootStrapper#getSpecifiedTokens) when it detects a token conflict; cleared
    // around every test so a value set by one test cannot leak into the next.
    private static final String REPLACE_ADDRESS_FIRST_BOOT = "cassandra.replace_address_first_boot";
    private static final String REPLACE_ADDRESS = "cassandra.replace_address";
    private static final String AUTO_REPLACE_PROPERTY = "cassandra.auto_replace";

    @Before
    public void clearReplacementProperties()
    {
        // auto_replace must come from the yaml config, not a leaked system property, and the replace
        // address must not survive between tests.
        System.clearProperty(AUTO_REPLACE_PROPERTY);
        System.clearProperty(REPLACE_ADDRESS_FIRST_BOOT);
        System.clearProperty(REPLACE_ADDRESS);
    }

    /**
     * The core path: a dead node is replaced by a new node that learned its token from the token service.
     * Pauses the replacement inside {@link BootStrapper#bootstrap} (which runs after BOOT_REPLACE has been
     * gossiped) to assert the live invariants, then releases it and confirms the data lands on the new node.
     */
    @Test
    public void autoReplaceUsesBootReplaceAndKeepsRangeAvailable() throws Exception
    {
        // 3 nodes, RF=3 so a single down node still leaves a QUORUM. The replacement (node 4) takes over
        // node 2's token slot (the token the fake token service will hand it).
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        ExecutorService startupExecutor = Executors.newSingleThreadExecutor();
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .withTokenSupplier(node -> even.token(node == REPLACEMENT_NUM ? NODE_TO_REPLACE_NUM : node))
                                      .withInstanceInitializer(BB::install)
                                      .start())
        {
            IInvokableInstance seed = cluster.get(SEED_NUM);
            IInvokableInstance nodeToReplace = cluster.get(NODE_TO_REPLACE_NUM);
            IInvokableInstance peer = cluster.get(PEER_NUM);

            cluster.setUncaughtExceptionsFilter((nodeId, cause) -> nodeId == NODE_TO_REPLACE_NUM);

            setupSchemaAndData(cluster);
            String assignedToken = Long.toString(even.token(NODE_TO_REPLACE_NUM));

            // take node 2 down and wait until the survivors agree it is down, otherwise the replacement
            // refuses to start.
            stopUnchecked(nodeToReplace);
            awaitRingStatus(seed, nodeToReplace, "Down");
            awaitRingStatus(peer, nodeToReplace, "Down");

            try (FakeTokenService tokenService = FakeTokenService.start(assignedToken);
                 WithProperties properties = new WithProperties())
            {
                managedReplacementProperties(properties, tokenService.url());

                IInvokableInstance replacement = addAutoReplaceInstance(cluster, nodeToReplace);
                String replacementHost = replacement.broadcastAddress().getAddress().getHostAddress();

                // start asynchronously: it will park inside BootStrapper#bootstrap (BB latch)
                java.util.concurrent.Future<?> startup = startupExecutor.submit((Runnable) replacement::startup);

                try
                {
                    awaitReached(SharedState.reachedBootstrap, startup);

                    // (1) peers see BOOT_REPLACE, never HIBERNATE
                    awaitGossipStatus(seed, replacement, "BOOT_REPLACE");
                    awaitGossipStatus(peer, replacement, "BOOT_REPLACE");
                    assertStatusIsBootReplaceNotHibernate(seed, replacementHost);
                    assertStatusIsBootReplaceNotHibernate(peer, replacementHost);

                    // (2) the range being taken over stays available while the replacement sits in
                    // BOOT_REPLACE. node 2 is down, but the survivors satisfy QUORUM.
                    Object[][] existing = seed.coordinator()
                                              .execute("SELECT pk FROM " + KEYSPACE + ".tbl", ConsistencyLevel.QUORUM);
                    Assertions.assertThat(existing.length)
                              .as("All previously written rows should be readable at QUORUM while the replacement is in BOOT_REPLACE")
                              .isEqualTo(10);

                    // a write made now must be forwarded to the BOOT_REPLACE node as a pending replica
                    // (the behaviour HIBERNATE suppresses); verified to have landed on it once it joins.
                    seed.coordinator().execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)", ConsistencyLevel.QUORUM, 100);
                }
                finally
                {
                    SharedState.releaseBootstrap.countDown();
                }

                startup.get(2, MINUTES);

                awaitRingJoin(seed, replacement);
                awaitRingJoin(replacement, seed);
                logger.info("Final ring: {}", awaitRingHealthy(seed));

                Set<String> expectedRing = hostSet(seed, peer, replacement);
                assertRingIs(seed, expectedRing);
                assertRingIs(peer, expectedRing);
                assertRingIs(replacement, expectedRing);

                assertLocalPks(replacement, pks(0, 10, 100));
            }
        }
        finally
        {
            startupExecutor.shutdownNow();
        }
    }

    /**
     * Ownership from the token service does not override liveness: if the token's current owner is still
     * alive, auto_replace must refuse rather than try to take over from a running node.
     */
    @Test
    public void autoReplaceRefusesToReplaceLiveNode() throws IOException
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .withTokenSupplier(node -> even.token(node == REPLACEMENT_NUM ? NODE_TO_REPLACE_NUM : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(SEED_NUM);
            IInvokableInstance liveOwner = cluster.get(NODE_TO_REPLACE_NUM);
            IInvokableInstance peer = cluster.get(PEER_NUM);

            cluster.setUncaughtExceptionsFilter((nodeId, cause) -> nodeId == REPLACEMENT_NUM);

            setupSchemaAndData(cluster);
            String assignedToken = Long.toString(even.token(NODE_TO_REPLACE_NUM));

            // node 2 is left ALIVE; the token service hands its token to the new node anyway.
            try (FakeTokenService tokenService = FakeTokenService.start(assignedToken))
            {
                IInvokableInstance replacement = addAutoReplaceInstance(cluster, liveOwner);
                Assertions.assertThatThrownBy(() -> ClusterUtils.start(replacement, props -> managedReplacementProperties(props, tokenService.url())))
                          .as("auto_replace must refuse to replace a live node")
                          .hasMessageContaining("Cannot replace a live node");
            }

            // the live owner is untouched and the ring is unchanged
            awaitRingHealthy(seed);
            Set<String> expectedRing = hostSet(seed, liveOwner, peer);
            assertRingIs(seed, expectedRing);
            assertRingIs(peer, expectedRing);
        }
    }

    /**
     * auto_replace must not over-trigger: when the token service assigns a token nobody owns, the node does
     * an ordinary bootstrap (cluster expansion), not a replacement, and no existing node is removed.
     */
    @Test
    public void autoReplaceWithoutConflictDoesNormalBootstrap() throws IOException
    {
        // size the token supplier for 4 evenly distributed tokens; nodes 1-3 take the first three and the
        // new node takes the fourth, which no live node owns.
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(4);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .withTokenSupplier(node -> even.token(node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(SEED_NUM);
            IInvokableInstance node2 = cluster.get(NODE_TO_REPLACE_NUM);
            IInvokableInstance peer = cluster.get(PEER_NUM);

            setupSchemaAndData(cluster);
            String unownedToken = Long.toString(even.token(REPLACEMENT_NUM));

            try (FakeTokenService tokenService = FakeTokenService.start(unownedToken))
            {
                IInvokableInstance joining = addAutoReplaceInstance(cluster, seed);
                // a plain bootstrap has no pre-bootstrap broadcast_interval wait, so give it a longer
                // ring_delay to gossip with the seed before the "seen any seed?" check.
                ClusterUtils.start(joining, props -> managedTokenServiceProperties(props, tokenService.url(), TimeUnit.SECONDS.toMillis(10)));

                awaitRingJoin(seed, joining);
                awaitRingJoin(joining, seed);
                logger.info("Final ring: {}", awaitRingHealthy(seed));

                // nothing was replaced: all three originals plus the new node are present
                Set<String> expectedRing = hostSet(seed, node2, peer, joining);
                assertRingIs(seed, expectedRing);
                assertRingIs(joining, expectedRing);
            }
        }
    }

    /**
     * Convergence under divergent gossip: a peer that is absent for the entire replacement (so it still
     * believes the dead node owns the token, and never sees the intermediate BOOT_REPLACE) must reconcile
     * to the new node owning that token once it comes back. This is the "some nodes have the old entry,
     * some don't" case the token service is supposed to make safe.
     */
    @Test
    public void autoReplaceConvergesOnPeerThatMissedReplacementGossip() throws Exception
    {
        TokenSupplier even = TokenSupplier.evenlyDistributedTokens(3);
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.with(Feature.GOSSIP, Feature.NETWORK)
                                                        .set(Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                      .withTokenSupplier(node -> even.token(node == REPLACEMENT_NUM ? NODE_TO_REPLACE_NUM : node))
                                      .start())
        {
            IInvokableInstance seed = cluster.get(SEED_NUM);
            IInvokableInstance nodeToReplace = cluster.get(NODE_TO_REPLACE_NUM);
            IInvokableInstance absentPeer = cluster.get(PEER_NUM);

            cluster.setUncaughtExceptionsFilter((nodeId, cause) -> nodeId == NODE_TO_REPLACE_NUM || nodeId == PEER_NUM);

            setupSchemaAndData(cluster);
            String assignedToken = Long.toString(even.token(NODE_TO_REPLACE_NUM));

            // node 2 is the dead node being replaced; node 3 is taken down so it is absent for the whole
            // replacement and keeps a stale view (node 2 still owns the token) until it comes back.
            stopUnchecked(nodeToReplace);
            awaitRingStatus(seed, nodeToReplace, "Down");
            stopUnchecked(absentPeer);

            IInvokableInstance replacement;
            try (FakeTokenService tokenService = FakeTokenService.start(assignedToken))
            {
                replacement = addAutoReplaceInstance(cluster, nodeToReplace);
                ClusterUtils.start(replacement, props -> managedReplacementProperties(props, tokenService.url()));

                // the seed sees the replacement join (the ring still lists the absent peer as down)
                awaitRingJoin(seed, replacement);
            }

            // BootStrapper set this on the replacement's behalf; clear it so the peer's restart below is a
            // plain restart and not mistaken for a replacement.
            System.clearProperty(REPLACE_ADDRESS_FIRST_BOOT);

            // the peer rejoins with a stale saved view (node 2 owning the token) and must reconcile it to
            // the replacement node owning that token.
            absentPeer.startup();

            awaitRingJoin(absentPeer, replacement);
            logger.info("Converged ring on the previously absent peer: {}", awaitRingHealthy(absentPeer));

            Set<String> expectedRing = hostSet(seed, absentPeer, replacement);
            assertRingIs(seed, expectedRing);
            assertRingIs(absentPeer, expectedRing);

            assertLocalPks(replacement, pks(0, 10));
        }
    }

    // ------------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------------

    private void setupSchemaAndData(Cluster cluster)
    {
        fixDistributedSchemas(cluster);
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE +
                             " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");
        for (int i = 0; i < 10; i++)
            cluster.coordinator(SEED_NUM).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)", ConsistencyLevel.ALL, i);
        cluster.forEach(i -> i.flush(KEYSPACE));
    }

    /**
     * Create (without starting) a replacement instance that enables {@code auto_replace} via the yaml config
     * and clears {@code initial_token} so the node falls through to the token service for its token.
     */
    private static IInvokableInstance addAutoReplaceInstance(Cluster cluster, IInstance base)
    {
        return ClusterUtils.addInstance(cluster, base.config(), c -> {
            c.set("auto_bootstrap", true);
            c.set("auto_replace", true); // yaml config setting, not the cassandra.auto_replace system property
            c.set("initial_token", null);
        });
    }

    /**
     * System properties for the managed/DGW replacement path: point the in-container TokenService at the
     * fake server, make a token-service assignment mandatory, and keep the pre-bootstrap waits short. The
     * replacement path sleeps {@code broadcast_interval} before bootstrap (long enough to gossip with a
     * seed), so a short ring_delay is fine here.
     */
    private static void managedReplacementProperties(WithProperties properties, String tokenServiceUrl)
    {
        managedTokenServiceProperties(properties, tokenServiceUrl, TimeUnit.SECONDS.toMillis(1));
    }

    private static void managedTokenServiceProperties(WithProperties properties, String tokenServiceUrl, long ringDelayMs)
    {
        properties.setProperty("netflix.tokenservice.url", tokenServiceUrl);
        properties.setProperty(DatabaseDescriptor.REQUIRE_ASSIGNED_TOKEN_PROPERTY, "true");
        // broadcast_interval must stay larger than ring_delay so a dead node's gossip ages out of the
        // "is it still alive?" window during prepareForBootstrap.
        properties.setProperty("cassandra.broadcast_interval_ms", Long.toString(TimeUnit.SECONDS.toMillis(5)));
        properties.setProperty("cassandra.ring_delay_ms", Long.toString(ringDelayMs));
        properties.set(BOOTSTRAP_SCHEMA_DELAY_MS, TimeUnit.SECONDS.toMillis(10));
        // the node being replaced is down, so its schema version cannot be fetched during the schema check
        properties.set(BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
    }

    private static void assertStatusIsBootReplaceNotHibernate(IInvokableInstance observer, String targetHost) throws InterruptedException
    {
        // poll, like awaitGossipStatus: a single gossipinfo snapshot can momentarily lack the STATUS field
        // for an endpoint while other application states are being applied.
        Map<String, Map<String, String>> lastGossip = null;
        for (int i = 0; i < 30; i++)
        {
            lastGossip = gossipInfo(observer);
            for (Map.Entry<String, Map<String, String>> entry : lastGossip.entrySet())
            {
                if (!entry.getKey().contains(targetHost))
                    continue;
                String status = entry.getValue().getOrDefault("STATUS_WITH_PORT", entry.getValue().get("STATUS"));
                if (status != null)
                {
                    Assertions.assertThat(status)
                              .as("Replacement %s gossip status as seen by %s", targetHost, observer)
                              .contains("BOOT_REPLACE")
                              .doesNotContain("hibernate");
                    return;
                }
            }
            TimeUnit.SECONDS.sleep(1);
        }
        throw new AssertionError("No gossip STATUS found for " + targetHost + " as seen by " + observer + "; gossip=" + lastGossip);
    }

    private static void assertLocalPks(IInvokableInstance instance, Set<Integer> expected)
    {
        Object[][] rows = instance.executeInternal("SELECT pk FROM " + KEYSPACE + ".tbl");
        Set<Integer> actual = new HashSet<>();
        for (Object[] row : rows)
            actual.add((Integer) row[0]);
        Assertions.assertThat(actual)
                  .as("Replacement node should locally hold the data it took over (plus any write forwarded during BOOT_REPLACE)")
                  .isEqualTo(expected);
    }

    private static Set<Integer> pks(int fromInclusive, int toExclusive, int... extra)
    {
        Set<Integer> pks = new HashSet<>();
        for (int i = fromInclusive; i < toExclusive; i++)
            pks.add(i);
        for (int e : extra)
            pks.add(e);
        return pks;
    }

    private static Set<String> hostSet(IInvokableInstance... instances)
    {
        Set<String> hosts = new HashSet<>();
        for (IInvokableInstance instance : instances)
            hosts.add(instance.broadcastAddress().getAddress().getHostAddress());
        return hosts;
    }

    private static void awaitReached(CountDownLatch latch, java.util.concurrent.Future<?> startup) throws Exception
    {
        for (int i = 0; i < 120; i++)
        {
            if (latch.await(1, TimeUnit.SECONDS))
                return;
            if (startup.isDone())
                startup.get(); // surface a startup failure instead of blocking forever
        }
        throw new AssertionError("Replacement node never reached the bootstrap/streaming step");
    }

    /**
     * Latches shared between the test (app classloader) and the ByteBuddy interceptor running inside the
     * replacement node's isolated classloader.
     */
    @Shared
    public static class SharedState
    {
        public static final CountDownLatch reachedBootstrap = new CountDownLatch(1);
        public static final CountDownLatch releaseBootstrap = new CountDownLatch(1);
    }

    public static class BB
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != REPLACEMENT_NUM)
                return;
            new ByteBuddy().rebase(BootStrapper.class)
                           .method(named("bootstrap"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static org.apache.cassandra.utils.concurrent.Future<StreamState> bootstrap(
            StreamStateStore stateStore, boolean useStrictConsistency,
            @SuperCall java.util.concurrent.Callable<org.apache.cassandra.utils.concurrent.Future<StreamState>> zuper) throws Exception
        {
            // By the time bootstrap() runs the node has already gossiped its BOOT_REPLACE status, so pause
            // here and let the test observe the cluster before streaming begins.
            SharedState.reachedBootstrap.countDown();
            SharedState.releaseBootstrap.await();
            return zuper.call();
        }
    }

    /**
     * Minimal stand-in for the Netflix token service. Only implements {@code GET /v1/token/current}, which
     * is what {@link org.apache.cassandra.config.DatabaseDescriptor#getInitialTokens()} calls (via
     * {@code TokenService.getCurrentInstance()}) when no initial_token is configured. Serves plain HTTP so
     * the production code skips Metatron mTLS, exactly like {@code local/LocalTokenServer.java}.
     */
    private static final class FakeTokenService implements AutoCloseable
    {
        private final HttpServer server;

        private FakeTokenService(HttpServer server)
        {
            this.server = server;
        }

        static FakeTokenService start(String token) throws IOException
        {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            String body = instanceJson(token);
            server.createContext("/v1/token/current", exchange -> {
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody())
                {
                    os.write(bytes);
                }
            });
            server.setExecutor(null);
            server.start();
            return new FakeTokenService(server);
        }

        String url()
        {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        // Field names mirror com.netflix.cassandra.NetflixInstance exactly; the production ObjectMapper
        // fails on unknown properties, so only emit fields that class declares.
        private static String instanceJson(String token)
        {
            return "{"
                   + "\"updateTime\":0,"
                   + "\"createdTime\":0,"
                   + "\"app\":\"cass_local\","
                   + "\"instanceId\":\"replacement\","
                   + "\"availabilityZone\":\"us-east-1a\","
                   + "\"token\":\"" + token + "\","
                   + "\"region\":\"us-east-1\","
                   + "\"id\":1,"
                   + "\"hostIP\":\"127.0.0." + REPLACEMENT_NUM + "\","
                   + "\"hostName\":\"replacement\","
                   + "\"key\":\"cass_local-replacement\""
                   + "}";
        }

        @Override
        public void close()
        {
            server.stop(0);
        }
    }
}
