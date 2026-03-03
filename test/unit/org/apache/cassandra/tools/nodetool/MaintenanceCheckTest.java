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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.MaintenanceCheckService;
import org.apache.cassandra.service.MaintenanceCheckService.StopResult;
import org.apache.cassandra.service.MaintenanceCheckService.StopResult.Status;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

public class MaintenanceCheckTest extends CQLTester
{
    private static final Logger logger = LoggerFactory.getLogger(MaintenanceCheckTest.class);

    private static final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    private static final VersionedValue.VersionedValueFactory valueFactory =
        new VersionedValue.VersionedValueFactory(partitioner);

    private static final String DC1 = SimpleSnitch.DATA_CENTER_NAME;
    private static final String DC2 = DC1.replace('1', '2');

    private static final String RACK1 = SimpleSnitch.RACK_NAME;
    private static final String RACK2 = SimpleSnitch.RACK_NAME.replace('1', '2');
    private static final String RACK3 = SimpleSnitch.RACK_NAME.replace('1', '3');
    private static final String[] RACKS = new String[]{RACK1, RACK2, RACK3};
    static
    {
        // Local node is always in SimpleSnitch.RACK_NAME
        Assertions.assertThat(rack(1)).isEqualTo(SimpleSnitch.RACK_NAME);
    }

    // Track synthetic nodes created per test so we can clean up between tests
    private final Set<InetAddressAndPort> syntheticNodes = new HashSet<>();
    private IEndpointSnitch originalSnitch;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        // Force class loading so the MBean is registered before any nodetool ToolRunner test
        MaintenanceCheckService.instance.getClass();
    }

    @After
    public void cleanupSyntheticNodes()
    {
        for (InetAddressAndPort addr : syntheticNodes)
        {
            Gossiper.instance.endpointStateMap.remove(addr);
            StorageService.instance.getTokenMetadata().removeEndpoint(addr);
        }
        syntheticNodes.clear();
        if (originalSnitch != null)
        {
            DatabaseDescriptor.setEndpointSnitch(originalSnitch);
            originalSnitch = null;
        }
    }

    /**
     * Install a snitch that reads DC/rack from Gossiper state, so NTS replica placement
     * and LOCAL_QUORUM filtering actually respect the DC assignments we inject.
     * SimpleSnitch ignores Gossiper and hardcodes "datacenter1" for all nodes.
     */
    private void installGossiperSnitch()
    {
        originalSnitch = DatabaseDescriptor.getEndpointSnitch();
        DatabaseDescriptor.setEndpointSnitch(new AbstractNetworkTopologySnitch()
        {
            public String getDatacenter(InetAddressAndPort endpoint)
            {
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (state == null || state.getApplicationState(ApplicationState.DC) == null)
                    throw new IllegalStateException("No DC in Gossiper for " + endpoint);
                return state.getApplicationState(ApplicationState.DC).value;
            }

            public String getRack(InetAddressAndPort endpoint)
            {
                EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
                if (state == null || state.getApplicationState(ApplicationState.RACK) == null)
                    throw new IllegalStateException("No rack in Gossiper for " + endpoint);
                return state.getApplicationState(ApplicationState.RACK).value;
            }
        });
    }

    /**
     * @return name of the generated keyspace
     */
    private String createNtsKeyspace(Map<String, Integer> dcRfs)
    {
        installGossiperSnitch();

        int totalRf = 0;

        int node = 1;

        // Create nodes separately, because we need node1 to be in the right DC + rack
        Assertions.assertThat(dcRfs).containsKey(DC1);
        int dc1Rf = dcRfs.get(DC1);
        for (int i = 0; i < dc1Rf; i++)
        {
            createSyntheticNode(DC1, rack(node), node);
            node++;
        }
        for (Map.Entry<String, Integer> entry : dcRfs.entrySet())
        {
            String dc = entry.getKey();
            int rf = entry.getValue();
            if (dc.equals(DC1))
                continue;
            for (int i = 0; i < rf; i++)
            {
                createSyntheticNode(dc, rack(node), node);
                node++;
            }
        }

        // For generating {'class': 'NetworkTopologyStrategy', 'dc1': 3}
        List<String> dcRfStrs = new ArrayList<>(dcRfs.size());
        for (Map.Entry<String, Integer> entry : dcRfs.entrySet())
        {
            String dc = entry.getKey();
            int rf = entry.getValue();

            totalRf += rf;

            // "'datacenter1': 3"
            String formatted = String.format("'%s': %s", dc, rf);
            dcRfStrs.add(formatted);
        }

        Assertions.assertThat(Gossiper.instance.endpointStateMap).hasSize(totalRf);

        String keyspace = createKeyspaceName();
        schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'NetworkTopologyStrategy', " + String.join(", ", dcRfStrs) + '}');
        logger.info("Created keyspace {}, with topology {}", keyspace, Gossiper.instance.endpointStateMap);
        return keyspace;
    }

    /**
     * @return name of the generated keyspace
     */
    private String createSimpleKeyspace(int rf)
    {
        for (int node = 1; node <= rf; node++)
            createSyntheticNode(SimpleSnitch.DATA_CENTER_NAME, SimpleSnitch.RACK_NAME, node);

        Assertions.assertThat(Gossiper.instance.endpointStateMap).hasSize(rf);

        String keyspace = createKeyspaceName();
        schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + '}');
        return keyspace;
    }

    private static String rack(int node)
    {
        return RACKS[((node - 1) % 3)];
    }

    private InetAddressAndPort addr(int node)
    {
        try
        {
            return InetAddressAndPort.getByName(String.format("127.0.0.%s", node));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void createSyntheticNode(String dc, String rack, int node)
    {
        logger.info("Creating node{} in dc {} rack {}", node, dc, rack);

        // node1 is the local node, already registered in Gossiper and TokenMetadata by CQLTester setup
        if (node == 1)
        {
            Assertions.assertThat(dc).isEqualTo(SimpleSnitch.DATA_CENTER_NAME);
            Assertions.assertThat(rack).isEqualTo(SimpleSnitch.RACK_NAME);
            return;
        }
        InetAddressAndPort addr = addr(node);
        syntheticNodes.add(addr);
        Token token = partitioner.getTokenFactory().fromString(Integer.toString(node));
        Gossiper.instance.initializeNodeUnsafe(addr, UUID.randomUUID(), MessagingService.current_version, 1);
        Gossiper.instance.injectApplicationState(addr, ApplicationState.TOKENS,
                                                 valueFactory.tokens(Collections.singleton(token)));
        Gossiper.instance.injectApplicationState(addr, ApplicationState.DC, valueFactory.datacenter(dc));
        Gossiper.instance.injectApplicationState(addr, ApplicationState.RACK, valueFactory.rack(rack));
        StorageService.instance.onChange(addr, ApplicationState.STATUS_WITH_PORT,
                                         valueFactory.normal(Collections.singleton(token)));
    }

    private void stopSyntheticNode(int node)
    {
        stopSyntheticNode(addr(node));
    }

    private void stopSyntheticNode(InetAddressAndPort addr)
    {
        Gossiper.runInGossipStageBlocking(() -> {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(addr);
            Assertions.assertThat(state).isNotNull();
            Gossiper.instance.markDead(addr, state);
        });
    }

    private void startSyntheticNode(int node)
    {
        InetAddressAndPort addr = addr(node);
        Gossiper.runInGossipStageBlocking(() -> {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(addr);
            Gossiper.instance.realMarkAlive(addr, state);
        });
    }

    private void decommissionSyntheticNode(int node)
    {
        InetAddressAndPort addr = addr(node);
        Token token = partitioner.getTokenFactory().fromString(Integer.toString(node));
        // Trigger LEAVING state transition: adds to TokenMetadata.leavingEndpoints.
        // The node stays alive (still serving reads) but the maintenance check treats it as unavailable
        // because it's a "time bomb" - once decommission completes, the node will stop serving.
        StorageService.instance.onChange(addr, ApplicationState.STATUS_WITH_PORT,
                                         valueFactory.leaving(Collections.singleton(token)));
    }

    /**
     * Transition an already-normal node to BOOTSTRAPPING state. handleStateBootstrap removes the
     * node from tokenToEndpointMap and adds it to bootstrapTokens (pending ranges).
     * The node accepts writes but not reads.
     */
    private void bootstrapSyntheticNode(int node)
    {
        InetAddressAndPort addr = addr(node);
        Token token = partitioner.getTokenFactory().fromString(Integer.toString(node));
        StorageService.instance.onChange(addr, ApplicationState.STATUS_WITH_PORT,
                                         valueFactory.bootstrapping(Collections.singleton(token)));
    }

    /**
     * Transition an already-normal node to MOVING state. handleStateMoving adds the node to
     * TokenMetadata.movingEndpoints. The node is changing token ownership — it still serves
     * its old ranges now, but will stop once the move completes.
     */
    private void moveSyntheticNode(int node, int newTokenValue)
    {
        InetAddressAndPort addr = addr(node);
        Token newToken = partitioner.getTokenFactory().fromString(Integer.toString(newTokenValue));
        StorageService.instance.onChange(addr, ApplicationState.STATUS_WITH_PORT,
                                         valueFactory.moving(newToken));
    }

    @Test
    public void testSafeAllNodesUp()
    {
        ToolRunner.ToolResult result = ToolRunner.invokeNodetool("maintenance-check", "stop", FBUtilities.getBroadcastAddressAndPort().toString(true));
        logger.info("Got stdout: {}", result.getStdout());
        logger.info("Got stderr: {}", result.getStderr());
        Assertions.assertThat(result.getExitCode()).isEqualTo(0);
        Assertions.assertThat(result.getStdout()).contains("Verdict:");
    }

    @Test
    public void testJsonFormat()
    {
        ToolRunner.ToolResult result = ToolRunner.invokeNodetool("maintenance-check", "stop", "-F", "json", FBUtilities.getBroadcastAddressAndPort().toString(true));
        Assertions.assertThat(result.getExitCode()).isEqualTo(0);
        Assertions.assertThat(result.getStdout()).contains("\"verdict\"");
    }

    @Test
    public void testYamlFormat()
    {
        ToolRunner.ToolResult result = ToolRunner.invokeNodetool("maintenance-check", "stop", "-F", "yaml", FBUtilities.getBroadcastAddressAndPort().toString(true));
        Assertions.assertThat(result.getExitCode()).isEqualTo(0);
        Assertions.assertThat(result.getStdout()).contains("verdict:");
    }

    @Test
    public void testInvalidFormat()
    {
        ToolRunner.ToolResult result = ToolRunner.invokeNodetool("maintenance-check", "stop", "-F", "xml", FBUtilities.getBroadcastAddressAndPort().toString(true));
        Assertions.assertThat(result.getExitCode()).isNotEqualTo(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckStopNonMember()
    {
        // 127.0.0.99 is not in the cluster
        MaintenanceCheckService.instance.checkStop(addr(99));
    }

    @Test(expected = IllegalStateException.class)
    public void testCheckStopBeforeInitialized() throws Exception
    {
        java.lang.reflect.Field f = StorageService.class.getDeclaredField("initialized");
        f.setAccessible(true);
        boolean prev = f.getBoolean(StorageService.instance);
        try
        {
            f.setBoolean(StorageService.instance, false);
            MaintenanceCheckService.instance.checkStop(FBUtilities.getBroadcastAddressAndPort());
        }
        finally
        {
            f.setBoolean(StorageService.instance, prev);
        }
    }

    @Test
    public void testSimpleUp()
    {
        createSimpleKeyspace(3);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(1));
        Assertions.assertThat(result.verdict).isTrue();
    }

    @Test
    public void testSimpleDown()
    {
        String keyspace = createSimpleKeyspace(3);
        stopSyntheticNode(2);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(1));
        Assertions.assertThat(result.verdict).isFalse();
        StopResult.ForKeyspace kr = result.keyspaceResults.get(keyspace);

        Assertions.assertThat(kr.consistency).isEqualTo("QUORUM");
        Assertions.assertThat(kr.required).isEqualTo(2);
        Assertions.assertThat(kr.alive).isEqualTo(1);
        Assertions.assertThat(kr.blockedBy).containsEntry(addr(2), "down");
    }

    @Test
    public void testSimpleDownThenUp()
    {
        createSimpleKeyspace(3);
        stopSyntheticNode(2);
        startSyntheticNode(2);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(1));
        Assertions.assertThat(result.verdict).isTrue();
    }

    @Test
    public void testNtsUp()
    {
        createNtsKeyspace(Map.of(DC1, 3, DC2, 3));
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(1));
        Assertions.assertThat(result.verdict).isTrue();
    }

    // NTS RF=3 in each of 2 DCs (6 total). DC1: nodes 1,2,3. DC2: nodes 4,5,6.
    // All DC2 nodes are down, checking whether we can stop node 1 (DC1).
    // LOCAL_QUORUM in DC1 is fine (2 of 3 alive after stopping node 1), but QUORUM across
    // all 6 replicas breaks (2 alive < 4 needed).
    @Test
    public void testNtsRemoteDcDown()
    {
        String keyspace = createNtsKeyspace(Map.of(DC1, 3, DC2, 3));

        // Stop everything in DC2
        for (Map.Entry<InetAddressAndPort, EndpointState> entry : Gossiper.instance.endpointStateMap.entrySet())
        {
            InetAddressAndPort addr = entry.getKey();
            EndpointState endpointState = entry.getValue();
            if (endpointState.getApplicationState(ApplicationState.DC).value.equals(DC2))
                stopSyntheticNode(addr);
        }

        StopResult result = MaintenanceCheckService.instance.checkStop(addr(1));
        Assertions.assertThat(result.verdict).isFalse();
        StopResult.ForKeyspace kr = result.keyspaceResults.get(keyspace);
        Assertions.assertThat(kr.status).isEqualTo(Status.UNSAFE);
        Assertions.assertThat(kr.message).contains("QUORUM");
        Assertions.assertThat(kr.message).doesNotContain("LOCAL_QUORUM");

        Assertions.assertThat(kr.consistency).isEqualTo("QUORUM");
        Assertions.assertThat(kr.required).isEqualTo(4);
        Assertions.assertThat(kr.alive).isEqualTo(2);
        Assertions.assertThat(kr.blockedBy).hasSize(3);
        Assertions.assertThat(kr.blockedBy).containsOnlyKeys(addr(4), addr(5), addr(6));
        Assertions.assertThat(kr.blockedBy).containsValue("down");
    }

    // SimpleStrategy RF=3, no nodes down. Verdict should be safe, but that keyspace should have WARNING_STRATEGY
    // because SimpleStrategy is not recommended for production.
    @Test
    public void testSimpleStrategyWarning()
    {
        String keyspace = createSimpleKeyspace(3);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(1));
        Assertions.assertThat(result.verdict).isTrue();
        Assertions.assertThat(result.keyspaceResults.get(keyspace).status).isEqualTo(Status.WARNING_STRATEGY);
    }

    // NetworkTopologyStrategy RF=2, no nodes down. That keyspace should have WARNING_LOW_RF because RF < 3
    // makes quorum checks meaningless. We don't assert on the overall verdict because system keyspaces
    // (e.g. system_distributed RF=3) may be UNSAFE with only 2 nodes in the cluster.
    @Test
    public void testNtsLowRfWarning()
    {
        String keyspace = createNtsKeyspace(Map.of(DC1, 2));
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(1));
        Assertions.assertThat(result.keyspaceResults.get(keyspace).status).isEqualTo(Status.WARNING_LOW_RF);
    }

    // NTS RF=3 in each of 2 DCs (6 total). DC1: nodes 1,2,3. DC2: nodes 4,5,6.
    // Nodes 5 and 6 are down, checking whether we can stop node 4.
    // Verdict should be false because LOCAL_QUORUM in DC2 would go down (0 of 3 alive).
    @Test
    public void testNtsTwoDownInRemoteDcUnsafe()
    {
        String keyspace = createNtsKeyspace(Map.of(DC1, 3, DC2, 3));
        stopSyntheticNode(5);
        stopSyntheticNode(6);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(4));
        Assertions.assertThat(result.verdict).isFalse();
        StopResult.ForKeyspace kr = result.keyspaceResults.get(keyspace);
        Assertions.assertThat(kr.status).isEqualTo(Status.UNSAFE);

        // LOCAL_QUORUM is checked from node 1's perspective (DC1), where all 3 nodes are up.
        // QUORUM across all 6 replicas breaks: 3 alive (DC1) < 4 required.
        Assertions.assertThat(kr.consistency).isEqualTo("QUORUM");
        Assertions.assertThat(kr.required).isEqualTo(4);
        Assertions.assertThat(kr.alive).isEqualTo(3);
        Assertions.assertThat(kr.blockedBy).containsEntry(addr(5), "down");
        Assertions.assertThat(kr.blockedBy).containsEntry(addr(6), "down");
    }

    // NTS RF=3 in 1 DC (3 total). DC1: nodes 1,2,3. Node 3 is down.
    // Checking whether we can stop node 2. Verdict should be false because LOCAL_QUORUM would go down
    // (only 1 of 3 replicas alive = node 1).
    @Test
    public void testNtsOneDownLocalDcUnsafe()
    {
        String keyspace = createNtsKeyspace(Map.of(DC1, 3));
        stopSyntheticNode(3);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(2));
        Assertions.assertThat(result.verdict).isFalse();
        StopResult.ForKeyspace kr = result.keyspaceResults.get(keyspace);
        Assertions.assertThat(kr.status).isEqualTo(Status.UNSAFE);

        Assertions.assertThat(kr.consistency).isEqualTo("LOCAL_QUORUM");
        Assertions.assertThat(kr.required).isEqualTo(2);
        Assertions.assertThat(kr.alive).isEqualTo(1);
        Assertions.assertThat(kr.blockedBy).containsExactly(Map.entry(addr(3), "down"));
    }

    @Test
    @Ignore("Use to regenerate docstring in MaintenanceCheckService")
    public void testNodetoolPlainOutput()
    {
        createNtsKeyspace(Map.of(DC1, 3));
        stopSyntheticNode(3);

        ToolRunner.ToolResult result = ToolRunner.invokeNodetool("maintenance-check", "stop", FBUtilities.getBroadcastAddressAndPort().toString(true));
        logger.info("Got stdout: {}", result.getStdout());
        logger.info("Got stderr: {}", result.getStderr());
    }

    // NTS RF=3 in each of 2 DCs (6 total). DC1: nodes 1,2,3. DC2: nodes 4,5,6.
    // Node 5 is down and node 6 is leaving, checking whether we can stop node 4.
    // Verdict should be false because LOCAL_QUORUM in DC2 would go down (0 of 3 alive).
    // Node 6 is leaving — it still serves reads now, but HypotheticalFailureDetector treats it as
    // unavailable because once decommission completes it'll stop. It's a time bomb.
    @Test
    public void testNtsLeavingNodeCountsAsUnavailable()
    {
        String keyspace = createNtsKeyspace(Map.of(DC1, 3, DC2, 3));
        stopSyntheticNode(5);
        decommissionSyntheticNode(6);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(4));
        Assertions.assertThat(result.verdict).isFalse();
        StopResult.ForKeyspace kr = result.keyspaceResults.get(keyspace);
        Assertions.assertThat(kr.status).isEqualTo(Status.UNSAFE);

        // LOCAL_QUORUM passes (DC1 fully up). QUORUM breaks: 3 alive (DC1) < 4 required.
        Assertions.assertThat(kr.consistency).isEqualTo("QUORUM");
        Assertions.assertThat(kr.required).isEqualTo(4);
        Assertions.assertThat(kr.alive).isEqualTo(3);
        Assertions.assertThat(kr.blockedBy).containsEntry(addr(5), "down");
        Assertions.assertThat(kr.blockedBy).containsEntry(addr(6), "leaving");
    }

    // NTS RF=3 in each of 2 DCs (6 total). DC1: nodes 1,2,3. DC2: nodes 4,5,6.
    // Node 5 is down and node 6 is moving to a new token, checking whether we can stop node 4.
    // Verdict should be false because LOCAL_QUORUM in DC2 would go down (0 of 3 alive).
    // A moving node still serves its old ranges, but once the move completes it will stop
    // serving them — same time bomb pattern as LEAVING.
    @Test
    public void testNtsMovingNodeCountsAsUnavailable()
    {
        String keyspace = createNtsKeyspace(Map.of(DC1, 3, DC2, 3));
        stopSyntheticNode(5);
        moveSyntheticNode(6, 100);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(4));
        Assertions.assertThat(result.verdict).isFalse();
        StopResult.ForKeyspace kr = result.keyspaceResults.get(keyspace);
        Assertions.assertThat(kr.status).isEqualTo(Status.UNSAFE);

        // LOCAL_QUORUM passes (DC1 fully up). QUORUM breaks: 3 alive (DC1) < 4 required.
        Assertions.assertThat(kr.consistency).isEqualTo("QUORUM");
        Assertions.assertThat(kr.required).isEqualTo(4);
        Assertions.assertThat(kr.alive).isEqualTo(3);
        Assertions.assertThat(kr.blockedBy).containsEntry(addr(5), "down");
        Assertions.assertThat(kr.blockedBy).containsEntry(addr(6), "moving");
    }

    // NTS RF=3 in each of 2 DCs (6 total). DC1: nodes 1,2,3. DC2: nodes 4,5,6.
    // Node 5 is down and node 6 is bootstrapping, checking whether we can stop node 4.
    // Verdict should be false because LOCAL_QUORUM for reads would break in DC2.
    //
    // A bootstrapping node lives in TokenMetadata.bootstrapTokens (pending ranges), NOT in
    // tokenToEndpointMap (normal members). ReplicaPlans.forRead() only considers natural replicas
    // (from tokenToEndpointMap), so node 6 is invisible to reads. Writes DO see pending replicas
    // via ReplicaLayout.forTokenWriteLiveAndDown(), but reads break first.
    //
    // With only nodes 4 and 5 as natural replicas in DC2, node 5 down, and node 4 as the
    // hypothetical stop target: 0 alive replicas in DC2, which fails LOCAL_QUORUM (needs 2).
    @Test
    public void testNtsBootstrappingNodeNotFullMember()
    {
        String keyspace = createNtsKeyspace(Map.of(DC1, 3, DC2, 3));
        bootstrapSyntheticNode(6);
        stopSyntheticNode(5);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(4));
        Assertions.assertThat(result.verdict).isFalse();
        // LOCAL_QUORUM passes (DC1 fully up). QUORUM breaks: node 6 not a natural replica
        // (bootstrapping), so only 5 natural replicas total. 3 alive (DC1) < 4 required.
        StopResult.ForKeyspace kr = result.keyspaceResults.get(keyspace);

        Assertions.assertThat(kr.consistency).isEqualTo("QUORUM");
        Assertions.assertThat(kr.required).isEqualTo(4);
        Assertions.assertThat(kr.alive).isEqualTo(3);
        Assertions.assertThat(kr.blockedBy).containsExactly(Map.entry(addr(5), "down"));
    }

    // NTS RF=3 in 1 DC. Nodes 1 and 2 are normal, node 3 is bootstrapping.
    // Checking whether we can stop node 2. Node 3 is in pending ranges (accepts writes)
    // but not in the normal token ring (invisible to reads). LOCAL_QUORUM needs 2 of 3,
    // but only node 1 would remain as a readable replica — unsafe.
    @Test
    public void testNtsBootstrappingNodeLocalDcUnsafe()
    {
        String keyspace = createNtsKeyspace(Map.of(DC1, 3));
        bootstrapSyntheticNode(3);
        StopResult result = MaintenanceCheckService.instance.checkStop(addr(2));
        Assertions.assertThat(result.verdict).isFalse();
        // Node 3 is bootstrapping (not a natural replica), so no nodes appear as blocking —
        // the failure is simply that there aren't enough natural replicas
        StopResult.ForKeyspace kr = result.keyspaceResults.get(keyspace);

        Assertions.assertThat(kr.consistency).isEqualTo("LOCAL_QUORUM");
        Assertions.assertThat(kr.required).isEqualTo(2);
        Assertions.assertThat(kr.alive).isEqualTo(1);
        Assertions.assertThat(kr.blockedBy).isEmpty();
    }
}