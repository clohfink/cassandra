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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import javax.management.openmbean.CompositeData;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.MaintenanceCheckService.StopResult.Status;
import org.apache.cassandra.service.reads.NeverSpeculativeRetryPolicy;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.MBeanWrapper;

/**
 * Check whether it's safe to take a node down for maintenance, based on the known state of peers on the current node.
 *
 * <pre>
 * $ nodetool maintenance-check stop 127.0.0.1:7012
 * Verdict: UNSAFE
 * Target: /127.0.0.1:7012
 *
 * Keyspace              Status         Message                            Consistency  Required Alive Blocked by
 * system_traces         WARNING_LOW_RF Low replication factor (RF=2)
 * system_distributed    UNSAFE         Would break QUORUM for reads       QUORUM       2        1     /127.0.0.3:7012 (down)
 * system_auth           WARNING_LOW_RF Low replication factor (RF=1)
 * cql_test_keyspace     WARNING_LOW_RF Low replication factor (RF=1)
 * cql_test_keyspace_alt WARNING_LOW_RF Low replication factor (RF=1)
 * keyspace_00           UNSAFE         Would break LOCAL_QUORUM for reads LOCAL_QUORUM 2        1     /127.0.0.3:7012 (down)
 * </pre>
 */
public class MaintenanceCheckService implements MaintenanceCheckServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=MaintenanceCheckService";

    public static final MaintenanceCheckService instance = new MaintenanceCheckService();

    private MaintenanceCheckService()
    {
        MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
    }

    public static class StopResult
    {
        public enum Status
        {
            // Stopping the node will not break quorum for this keyspace.
            SAFE,
            // Stopping the node would break LOCAL_QUORUM or QUORUM
            UNSAFE,
            // Replication factor is too low (< 3) to make a meaningful quorum check
            WARNING_LOW_RF,
            // Keyspace uses SimpleStrategy (or unknown strategy) but would otherwise be safe
            WARNING_STRATEGY
        }

        public static class ForKeyspace
        {
            public final Status status;
            public final String message;
            public final String consistency;
            public final Integer required;
            public final Integer alive;
            public final Map<InetAddressAndPort, String> blockedBy;

            public ForKeyspace(Status status, String message)
            {
                this(status, message, null, null);
            }

            public ForKeyspace(Status status, String message, UnavailableException cause, Map<InetAddressAndPort, String> blockedBy)
            {
                this.status = status;
                this.message = message;
                this.consistency = cause != null ? cause.consistency.name() : null;
                this.required = cause != null ? cause.required : null;
                this.alive = cause != null ? cause.alive : null;
                this.blockedBy = blockedBy;
            }
        }

        public boolean verdict = true;
        public final InetAddressAndPort target;
        public final Map<String, ForKeyspace> keyspaceResults = new LinkedHashMap<>();

        public StopResult(InetAddressAndPort target)
        {
            this.target = target;
        }

        public void add(String keyspace, ForKeyspace kr)
        {
            keyspaceResults.put(keyspace, kr);
            if (kr.status == Status.UNSAFE)
                verdict = false;
        }
    }

    /**
     * Check whether stopping the given node would break QUORUM / LOCAL_QUORUM
     * for any replicated keyspace.
     *
     * Groups keyspaces by their {@link ReplicationParams} so that keyspaces
     * sharing identical replication are only checked once.
     */
    public StopResult checkStop(InetAddressAndPort target)
    {
        if (!StorageService.instance.isInitialized())
            throw new IllegalStateException("Not yet initialized, can't perform maintenance check");

        if (!StorageService.instance.getTokenMetadata().isMember(target))
            throw new IllegalArgumentException("Node " + target + " is not a member of the cluster");

        StopResult result = new StopResult(target);

        // Group keyspaces by ReplicationParams so identical replication is checked once
        Map<ReplicationParams, List<String>> byReplication = new LinkedHashMap<>();
        for (String ksName : Schema.instance.getKeyspaces())
        {
            KeyspaceMetadata ksMeta = Schema.instance.getKeyspaceMetadata(ksName);
            if (ksMeta == null)
                continue;
            AbstractReplicationStrategy strategy = Keyspace.open(ksName).getReplicationStrategy();
            if (strategy instanceof LocalStrategy || strategy.getReplicationFactor().allReplicas == 0)
                continue;
            byReplication.computeIfAbsent(ksMeta.params.replication, k -> new ArrayList<>()).add(ksName);
        }

        // Check each unique ReplicationParams once, apply the result to all its keyspaces
        for (Map.Entry<ReplicationParams, List<String>> entry : byReplication.entrySet())
        {
            List<String> keyspaceNames = entry.getValue();
            // Use the first keyspace to open the strategy — all share the same replication
            Keyspace ks = Keyspace.open(keyspaceNames.get(0));
            StopResult.ForKeyspace templateResult = checkKeyspaceSafety(ks, target);

            for (String keyspace : keyspaceNames)
            {
                result.add(keyspace, templateResult);
            }
        }

        return result;
    }

    /**
     * Factory that creates predicates simulating what happens if the target node is stopped.
     * Each predicate wraps the real FailureDetector and additionally treats LEAVING/MOVING
     * nodes as unavailable since they will stop serving once their operation completes.
     * The returned {@link TrackingPredicate} records which nodes it considers down (and why)
     * so they can be included in the response output.
     */
    private static class HypotheticalFailurePredicateFactory
    {
        private final IFailureDetector delegate;
        private final InetAddressAndPort target;

        HypotheticalFailurePredicateFactory(IFailureDetector delegate, InetAddressAndPort target)
        {
            this.delegate = delegate;
            this.target = target;
        }

        TrackingPredicate create()
        {
            return new TrackingPredicate();
        }

        class TrackingPredicate implements Predicate<Replica>
        {
            final Map<InetAddressAndPort, String> blockedBy = new HashMap<>();

            @Override
            public boolean test(Replica replica)
            {
                InetAddressAndPort ep = replica.endpoint();
                if (ep.equals(target))
                    return false;
                if (!delegate.isAlive(ep))
                {
                    blockedBy.putIfAbsent(ep, "down");
                    return false;
                }
                if (StorageService.instance.getTokenMetadata().isLeaving(ep))
                {
                    blockedBy.putIfAbsent(ep, "leaving");
                    return false;
                }
                if (StorageService.instance.getTokenMetadata().isMoving(ep))
                {
                    blockedBy.putIfAbsent(ep, "moving");
                    return false;
                }
                return true;
            }
        }
    }

    /**
     * Check whether stopping the target node would break quorum for a single keyspace.
     */
    private StopResult.ForKeyspace checkKeyspaceSafety(Keyspace ks, InetAddressAndPort target)
    {
        AbstractReplicationStrategy strategy = ks.getReplicationStrategy();
        int rf = strategy.getReplicationFactor().allReplicas;

        if (rf < 3)
        {
            return new StopResult.ForKeyspace(Status.WARNING_LOW_RF, "Low replication factor (RF=" + rf + ")");
        }

        boolean isNts = strategy instanceof NetworkTopologyStrategy;
        // LOCAL_QUORUM is only meaningful for DC-aware strategies; QUORUM applies to all
        ConsistencyLevel[] levels = isNts
            ? new ConsistencyLevel[]{ ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.QUORUM }
            : new ConsistencyLevel[]{ ConsistencyLevel.QUORUM };

        HypotheticalFailurePredicateFactory factory = new HypotheticalFailurePredicateFactory(FailureDetector.instance, target);
        for (Token token : sampleTokens(ks, target))
        {
            StopResult.ForKeyspace failure = checkToken(ks, token, levels, factory);
            if (failure != null)
                return failure;
        }

        if (!isNts)
            return new StopResult.ForKeyspace(Status.WARNING_STRATEGY, "Uses " + strategy.getClass().getSimpleName() + " (RF=" + rf + ")");

        return new StopResult.ForKeyspace(Status.SAFE, "Safe to stop (RF=" + rf + ")");
    }

    /**
     * Generate sample tokens across all ranges that the target node replicates
     * for the given keyspace. For each range, emits both boundary tokens
     * (left.increaseSlightly and right) to catch pending replicas that may
     * differ at range boundaries (e.g. a bootstrapping node taking part of a range).
     */
    @VisibleForTesting
    public Iterable<Token> sampleTokens(Keyspace ks, InetAddressAndPort target)
    {
        RangesAtEndpoint replicas = StorageService.instance.getReplicas(ks.getReplicationStrategy(), target);
        Iterable<List<Token>> perRange = Iterables.transform(replicas,
            r -> List.of(r.range().left.increaseSlightly(), r.range().right));
        return Iterables.concat(perRange);
    }

    /**
     * Check that reads and writes would still satisfy LOCAL_QUORUM and QUORUM for a single token.
     * Reads are checked first since they use only natural replicas (writes include pending replicas
     * too, so reads always break before writes for the same consistency level).
     * Returns a ForKeyspace with UNSAFE status if quorum would be broken, or null if safe.
     */
    private StopResult.ForKeyspace checkToken(Keyspace ks, Token token, ConsistencyLevel[] levels,
                                              HypotheticalFailurePredicateFactory factory)
    {
        HypotheticalFailurePredicateFactory.TrackingPredicate aliveForReads = factory.create();
        for (ConsistencyLevel cl : levels)
        {
            try
            {
                ReplicaPlans.forRead(ks, token, cl, NeverSpeculativeRetryPolicy.INSTANCE, aliveForReads);
            }
            catch (UnavailableException e)
            {
                return new StopResult.ForKeyspace(Status.UNSAFE, "Would break " + cl + " for reads", e, aliveForReads.blockedBy);
            }
        }

        HypotheticalFailurePredicateFactory.TrackingPredicate aliveForWrites = factory.create();
        for (ConsistencyLevel cl : levels)
        {
            try
            {
                ReplicaLayout.ForTokenWrite liveAndDown = ReplicaLayout.forTokenWriteLiveAndDown(ks, token);
                ReplicaPlans.forWrite(ks, cl, liveAndDown, aliveForWrites, ReplicaPlans.writeAll);
            }
            catch (UnavailableException e)
            {
                return new StopResult.ForKeyspace(Status.UNSAFE, "Would break " + cl + " for writes", e, aliveForWrites.blockedBy);
            }
        }

        return null;
    }

    /**
     * JMX-accessible wrapper for {@link #checkStop(InetAddressAndPort)}.
     * Returns a {@link javax.management.openmbean.CompositeData} with verdict, target,
     * and per-keyspace results as {@link javax.management.openmbean.TabularData}.
     */
    @Override
    public CompositeData checkStop(String endpoint)
    {
        InetAddressAndPort target;
        try
        {
            target = InetAddressAndPort.getByName(endpoint);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        return MaintenanceCheckCompositeData.from(checkStop(target));
    }
}
