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
package org.apache.cassandra.antithesis.tcm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.antithesis.sdk.Assert;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.apache.cassandra.antithesis.tcm.Harness.Log;
import org.apache.cassandra.antithesis.tcm.Harness.Node;

/**
 * Workload-side property checks. One method per property; the slug in each javadoc matches
 * {@code antithesis/scratchbook/property-catalog.md} and its evidence file under
 * {@code scratchbook/properties/}.
 *
 * <p>Two rules apply throughout, and getting either wrong makes the whole suite untrustworthy:
 *
 * <ol>
 *   <li><b>Only evaluate over nodes that answered.</b> A node behind an injected partition supplies
 *       no evidence. Treating silence as agreement hides failures; treating it as disagreement
 *       makes every partition a violation. Each check reports how many nodes it managed to read so
 *       {@link #allNodesCompared} can surface runs where comparisons were persistently incomplete.
 *   <li><b>Compare within an epoch, never across.</b> Nodes at different epochs are supposed to
 *       differ. Every cross-node check either intersects on epoch or groups by it, which removes
 *       replication lag as a source of false failures entirely.
 * </ol>
 */
public final class Checks
{
    // Assertion messages must be compile-time constants: each distinct message becomes its own
    // Antithesis test property, so they are declared once here and never built by concatenation.
    private static final String LOG_AGREEMENT =
        "TCM log entries agree across nodes at the same epoch";
    private static final String EPOCH_MONOTONIC =
        "TCM epoch observed by client is non-decreasing per node";
    private static final String SINGLE_CMS_ID =
        "all nodes report a single cluster metadata identifier";
    private static final String CMS_NON_EMPTY =
        "CMS member set is non-empty after initialization";
    private static final String INIT_UNIFORM =
        "aborted CMS initialization leaves no partial CMS state";
    private static final String SCHEMA_AGREEMENT =
        "nodes at the same epoch report the same schema version";
    private static final String RF_PRESERVED =
        "every range retains at least RF write replicas at every epoch";
    private static final String RING_COVERED =
        "placements cover the whole ring with no gaps";
    private static final String PEERS_MATCH_DIRECTORY =
        "system.peers_v2 agrees with the cluster metadata directory";
    private static final String COMMIT_EXACTLY_ONCE =
        "every committed transformation appears exactly once in the log";
    private static final String AVAILABLE_DURING_CHURN =
        "a QUORUM read and write completed during churn";
    // "two or more multi-step operations were in flight at once" is asserted in
    // Actions.launchConcurrentMovements(), which owns that message. concurrentSequences() here only
    // observes/records the peak; it does not assert, so the constant is not needed in this file.
    private static final String RECONFIGURATION_OBSERVED =
        "a CMS reconfiguration was observed in progress";
    private static final String ALL_NODES_COMPARED =
        "a cross-node comparison included every node";
    private static final String CONVERGED =
        "all live nodes converge to an identical cluster metadata epoch";
    private static final String SEQUENCES_DRAINED =
        "all in-progress sequences drain after faults stop";
    private static final String CMS_ACCEPTS_COMMIT =
        "CMS accepts a new transformation after recovery";

    private final Harness harness;
    /** Highest node count any cross-node check reached this cycle; drives allNodesCompared. */
    private int maxNodesCompared = 0;

    public Checks(Harness harness)
    {
        this.harness = harness;
    }

    // -------------------------------------------------------------------------------------------
    // Snapshot of what every reachable node currently believes
    // -------------------------------------------------------------------------------------------

    /** Per-node {@code describeCMS()} output, keyed by host. Unreachable nodes are absent. */
    public Map<String, Map<String, String>> cmsSnapshot()
    {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        for (Node n : harness.nodes)
        {
            Map<String, String> info = n.describeCMS();
            if (info != null && !info.isEmpty())
                out.put(n.host, info);
        }
        record(out.size());
        return out;
    }

    private void record(int nodesRead)
    {
        maxNodesCompared = Math.max(maxNodesCompared, nodesRead);
    }

    private static long epochOf(Map<String, String> cms)
    {
        try
        {
            return Long.parseLong(cms.getOrDefault("EPOCH", "-1"));
        }
        catch (NumberFormatException e)
        {
            return -1;
        }
    }

    // -------------------------------------------------------------------------------------------
    // Category A
    // -------------------------------------------------------------------------------------------

    /**
     * {@code a-log-prefix-agreement} -- two nodes never disagree about what an epoch means.
     *
     * <p><b>Not implemented via {@code dumpLog}, deliberately.</b> {@code CMSOperations.dumpLog}
     * delegates to {@code ClusterMetadataLogTable.log()}, which issues
     * {@code SELECT ... FROM system_cluster_metadata.distributed_metadata_log} at
     * {@code ConsistencyLevel.QUORUM}. Calling it on five nodes therefore returns the CMS's single
     * authoritative view five times, and comparing those to each other can never fail. That was
     * flagged as a risk in this property's evidence file and turned out to be the case.
     *
     * <p>Implemented instead against state that genuinely is per-node.
     * {@code CMSOperations.dumpDirectory} is backed by {@code ClusterMetadata.current()} on the node
     * being asked (see {@code ClusterMetadataDirectoryTable.directory()}), so it is that node's own
     * belief. Since {@code ClusterMetadata} is a deterministic function of the log prefix, two nodes
     * reporting the same epoch must report identical directories -- so a directory disagreement
     * within one epoch <em>is</em> a log disagreement, observed through its consequence.
     *
     * <p>This covers the directory component (node identity, state, tokens, in-flight sequences).
     * Schema is covered separately by {@link #schemaAgreementAtSameEpoch()}. Direct entry-by-entry
     * comparison of each node's local log would need new observability -- a JMX method exposing
     * {@code LocalLog}'s own view rather than the distributed table -- which is recorded in the
     * evidence file as the way to strengthen this.
     */
    public void logPrefixAgreement(Map<String, Map<String, String>> snapshot)
    {
        // Group nodes by the epoch they report, then compare local metadata within each group.
        // Grouping by epoch is what removes replication lag as a source of false failures: nodes at
        // different epochs are supposed to differ.
        Map<Long, Map<String, String>> directoryByEpoch = new TreeMap<>();
        Map<Long, List<String>> hostsByEpoch = new TreeMap<>();
        int read = 0;

        for (Map.Entry<String, Map<String, String>> e : snapshot.entrySet())
        {
            Node n = harness.node(e.getKey());
            long epoch = epochOf(e.getValue());
            if (epoch < 1)
                continue;

            Map<Long, Map<String, String>> dir = n.dumpDirectory(true);
            if (dir == null || dir.isEmpty())
                continue;

            // Re-read the epoch afterwards: if it moved, the directory sample straddled an
            // enactment and pairing it with the earlier epoch would be wrong.
            Map<String, String> after = n.describeCMS();
            if (after == null || epochOf(after) != epoch)
                continue;

            read++;
            String rendered = canonicalDirectory(dir);
            hostsByEpoch.computeIfAbsent(epoch, k -> new ArrayList<>()).add(e.getKey());

            String existing = directoryByEpoch.get(epoch) == null
                              ? null
                              : directoryByEpoch.get(epoch).get("rendered");
            if (existing == null)
            {
                Map<String, String> holder = new LinkedHashMap<>();
                holder.put("rendered", rendered);
                holder.put("host", e.getKey());
                directoryByEpoch.put(epoch, holder);
            }
            else
            {
                boolean agree = existing.equals(rendered);

                ObjectNode d = Harness.details();
                d.put("epoch", epoch);
                d.put("node_a", directoryByEpoch.get(epoch).get("host"));
                d.put("node_b", e.getKey());
                d.put("directory_entries_a", existing.length());
                d.put("directory_entries_b", rendered.length());
                d.put("nodes_at_this_epoch", hostsByEpoch.get(epoch).size());
                if (!agree)
                {
                    // Only on failure: these are large, and a triage report is more useful with them
                    // than without.
                    d.put("directory_a", existing);
                    d.put("directory_b", rendered);
                }
                Assert.always(agree, LOG_AGREEMENT, d);

                if (!agree)
                {
                    record(read);
                    return; // one report is enough; keep the triage signal clean
                }
            }
        }
        record(read);
        harness.state.bump("log_agreement_checks");
    }

    /**
     * {@code a-epoch-monotonic-per-node} -- the epoch a node advertises never goes backwards,
     * including across a restart.
     *
     * <p>High-water marks are keyed per host and persisted, because a node legitimately lags its
     * peers and because test commands run as separate processes. This is the half of the property
     * the SUT-side assertion at the publication CAS cannot see.
     */
    public void epochMonotonic(Map<String, Map<String, String>> snapshot)
    {
        for (Map.Entry<String, Map<String, String>> e : snapshot.entrySet())
        {
            String host = e.getKey();
            long epoch = epochOf(e.getValue());
            if (epoch < 0)
                continue;

            Long previous = harness.state.highestEpochSeen.get(host);
            boolean ok = previous == null || epoch >= previous;

            ObjectNode d = Harness.details();
            d.put("node", host);
            d.put("observed_epoch", epoch);
            d.put("highest_previously_seen", previous == null ? -1L : previous);
            d.put("service_state", e.getValue().get("SERVICE_STATE"));
            Assert.always(ok, EPOCH_MONOTONIC, d);

            if (previous == null || epoch > previous)
                harness.state.highestEpochSeen.put(host, epoch);
        }
    }

    /**
     * {@code a-metadata-identifier-unique} -- one cluster, one metadata service.
     *
     * <p>Guarded on the node's own {@code EPOCH >= 1}: before initialisation every node reports
     * CMS_ID 0 legitimately, which is the same guard {@code describeCMS} itself applies when it
     * blanks REPLICATION_FACTOR below {@code Epoch.FIRST}.
     */
    public void singleMetadataIdentifier(Map<String, Map<String, String>> snapshot)
    {
        Map<String, String> idByHost = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, String>> e : snapshot.entrySet())
        {
            if (epochOf(e.getValue()) >= 1)
                idByHost.put(e.getKey(), e.getValue().get("CMS_ID"));
        }
        if (idByHost.isEmpty())
            return;

        Set<String> distinct = new LinkedHashSet<>(idByHost.values());
        boolean ok = distinct.size() == 1 && !distinct.contains("0") && !distinct.contains(null);

        ObjectNode d = Harness.details();
        d.put("distinct_identifier_count", distinct.size());
        d.set("identifier_by_node", jsonOf(idByHost));
        Assert.always(ok, SINGLE_CMS_ID, d);
    }

    // -------------------------------------------------------------------------------------------
    // Category C
    // -------------------------------------------------------------------------------------------

    /** {@code c-cms-membership-never-empty} -- the cluster never loses its metadata service. */
    public void cmsMembershipNonEmpty(Map<String, Map<String, String>> snapshot)
    {
        for (Map.Entry<String, Map<String, String>> e : snapshot.entrySet())
        {
            if (epochOf(e.getValue()) < 1)
                continue; // pre-initialisation and post-upgrade have no members, by design

            String members = e.getValue().getOrDefault("MEMBERS", "");
            boolean ok = !members.trim().isEmpty();

            ObjectNode d = Harness.details();
            d.put("node", e.getKey());
            d.put("members", members);
            d.put("epoch", epochOf(e.getValue()));
            d.put("service_state", e.getValue().get("SERVICE_STATE"));
            d.put("is_migrating", e.getValue().get("IS_MIGRATING"));
            d.put("needs_reconfiguration", e.getValue().get("NEEDS_RECONFIGURATION"));
            Assert.always(ok, CMS_NON_EMPTY, d);
        }
    }

    /**
     * {@code c-initialization-abort-recoverable} -- no node holds partial CMS state.
     *
     * <p>Asserts uniformity rather than a particular state: both "all initialized" and "all
     * uninitialized" are valid, and only the mixture is unrecoverable.
     */
    public void initializationUniform(Map<String, Map<String, String>> snapshot)
    {
        if (snapshot.size() < 2)
            return;

        Set<Boolean> initialized = new LinkedHashSet<>();
        ObjectNode perNode = Harness.details();
        for (Map.Entry<String, Map<String, String>> e : snapshot.entrySet())
        {
            String id = e.getValue().getOrDefault("CMS_ID", "0");
            boolean isInit = epochOf(e.getValue()) >= 1 && !"0".equals(id);
            initialized.add(isInit);
            ObjectNode row = perNode.putObject(e.getKey());
            row.put("initialized", isInit);
            row.put("cms_id", id);
            row.put("epoch", epochOf(e.getValue()));
        }

        boolean ok = initialized.size() == 1;
        ObjectNode d = Harness.details();
        d.put("nodes_compared", snapshot.size());
        d.set("per_node", perNode);
        Assert.always(ok, INIT_UNIFORM, d);
    }

    /**
     * {@code r-cms-reconfiguration-observed} -- reachability guard for the two CMS safety
     * properties, which are {@code AlwaysOrUnreachable} and pass vacuously if reconfiguration
     * never runs.
     */
    public void reconfigurationObserved(Map<String, Map<String, String>> snapshot)
    {
        boolean migrating = false;
        for (Node n : harness.nodes)
        {
            Map<String, List<String>> status = n.reconfigureCMSStatus();
            if (status != null && !status.isEmpty())
            {
                migrating = true;
                break;
            }
        }
        if (!migrating)
        {
            for (Map<String, String> cms : snapshot.values())
            {
                if ("true".equalsIgnoreCase(cms.getOrDefault("IS_MIGRATING", "false")))
                {
                    migrating = true;
                    break;
                }
            }
        }
        if (migrating)
            harness.state.bump("reconfigurations_observed_in_flight");

        ObjectNode d = Harness.details();
        d.put("observed_in_flight_total",
              harness.state.counter("reconfigurations_observed_in_flight"));
        d.put("reconfigurations_requested",
              harness.state.counter("reconfigure_requests"));
        Assert.sometimes(migrating, RECONFIGURATION_OBSERVED, d);
    }

    /**
     * {@code c-commit-survives-cms-membership-change} -- exactly-once, plus the rejection
     * direction folded in per evaluation refinement R8.
     *
     * <p>The ledger distinguishes acked / unknown / rejected. A timeout is recorded as unknown and
     * never as failed: collapsing the two is what turns this into a false-positive generator under
     * partition.
     */
    public void commitLedgerExactlyOnce()
    {
        if (harness.state.tagLedger.isEmpty())
            return;

        // Any single reachable node's log is enough: this asks about the log's content, not about
        // agreement between nodes (that is a-log-prefix-agreement).
        Map<Long, Map<String, String>> log = null;
        String readFrom = null;
        for (Node n : harness.nodes)
        {
            log = n.dumpLog(1L, Long.MAX_VALUE);
            if (log != null && !log.isEmpty())
            {
                readFrom = n.host;
                break;
            }
        }
        if (log == null || log.isEmpty())
            return;

        // Count occurrences of each tag across all rendered transformations.
        Map<String, Integer> occurrences = new HashMap<>();
        for (String tag : harness.state.tagLedger.keySet())
            occurrences.put(tag, 0);
        for (Map<String, String> entry : log.values())
        {
            String rendered = String.valueOf(entry.get("transformation"));
            for (String tag : occurrences.keySet())
            {
                if (rendered.contains(tag))
                    occurrences.merge(tag, 1, Integer::sum);
            }
        }

        for (Map.Entry<String, String> led : harness.state.tagLedger.entrySet())
        {
            String tag = led.getKey();
            String outcome = led.getValue();
            int count = occurrences.getOrDefault(tag, 0);

            // The sound invariants, and only these:
            //   * acked  => present exactly once (at-least-once AND at-most-once), and
            //   * ANY outcome => present at most once (never a double-commit).
            //
            // What is NOT sound: "a client-observed rejection means the CMS did not persist it."
            // A client-side exception type cannot distinguish "CMS rejected, not persisted" from
            // "CMS committed, the ack was lost/delayed". Run 32d96d63...-59-13 produced exactly that
            // false positive under heavy churn: a CREATE TABLE surfaced an InvalidQueryException yet
            // the table was present once (outcome=rejected, occ=1) -- committed, ack failed, not a
            // TCM bug. So a non-acked outcome only bounds occ to <= 1. The authoritative "a
            // transformation was rejected" signal is the SUT-side meter/assert in
            // AbstractLocalProcessor, not the driver exception. No counterexample in that run had
            // occ>=2 or acked&occ=0, i.e. no real double-commit or lost commit.
            boolean ok;
            if ("acked".equals(outcome))
                ok = count == 1;   // exactly once
            else
                ok = count <= 1;   // unknown/rejected/timeout: at most once (never double)

            ObjectNode d = Harness.details();
            d.put("tag", tag);
            d.put("recorded_outcome", outcome);
            d.put("occurrences_in_log", count);
            d.put("log_read_from", readFrom);
            Assert.always(ok, COMMIT_EXACTLY_ONCE, d);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Category D
    // -------------------------------------------------------------------------------------------

    /**
     * {@code d-schema-agreement-at-same-epoch} -- same epoch implies same schema version.
     *
     * <p>Samples schema, then epoch, then schema again, and discards the sample if the two schema
     * reads disagree: that means the sample straddled an enactment, which would otherwise produce
     * occasional false failures.
     */
    public void schemaAgreementAtSameEpoch()
    {
        Map<Long, Map<String, String>> byEpoch = new TreeMap<>();
        int read = 0;
        for (Node n : harness.nodes)
        {
            String before = n.schemaVersion();
            Map<String, String> cms = n.describeCMS();
            String after = n.schemaVersion();
            if (before == null || after == null || cms == null || !before.equals(after))
                continue; // unreachable, or the sample straddled an epoch change

            long epoch = epochOf(cms);
            if (epoch < 1)
                continue;
            byEpoch.computeIfAbsent(epoch, k -> new LinkedHashMap<>()).put(n.host, before);
            read++;
        }
        record(read);

        for (Map.Entry<Long, Map<String, String>> group : byEpoch.entrySet())
        {
            if (group.getValue().size() < 2)
                continue; // a single node at an epoch is no evidence

            Set<String> distinct = new LinkedHashSet<>(group.getValue().values());
            boolean ok = distinct.size() == 1;

            ObjectNode d = Harness.details();
            d.put("epoch", group.getKey());
            d.put("nodes_at_epoch", group.getValue().size());
            d.put("distinct_schema_versions", distinct.size());
            d.set("schema_by_node", jsonOf(group.getValue()));
            Assert.always(ok, SCHEMA_AGREEMENT, d);
        }
    }

    /**
     * {@code b-replication-factor-never-under} and {@code d-ring-fully-owned}.
     *
     * <p>Both read Cassandra's own computed replica sets via
     * {@code getRangeToEndpointWithPortMap} rather than reconstructing placements from tokens
     * (evaluation refinement R1) -- reconstruction would mean reimplementing the placement
     * algorithm here and then testing the reimplementation.
     *
     * <p>The RF floor is {@code min(RF, liveRegisteredNodes)} so that operator-caused
     * under-replication (the workload decommissioned too far) is not reported as a protocol defect.
     */
    public void placements(Map<String, Map<String, String>> snapshot, int sequencesInFlight)
    {
        int liveNodes = 0;
        for (Node n : harness.nodes)
            if (n.agentSaysRunning())
                liveNodes++;
        if (liveNodes == 0)
            liveNodes = snapshot.size();
        int floor = Math.min(Harness.PROBE_RF, Math.max(liveNodes, 1));

        int read = 0;
        for (Node n : harness.nodes)
        {
            Map<List<String>, List<String>> ranges = n.rangeToEndpointMap(Harness.PROBE_KEYSPACE);
            if (ranges == null || ranges.isEmpty())
                continue;
            read++;

            Map<List<String>, List<String>> pending =
                n.pendingRangeToEndpointMap(Harness.PROBE_KEYSPACE);

            // --- RF floor ---
            for (Map.Entry<List<String>, List<String>> range : ranges.entrySet())
            {
                Set<String> replicas = new LinkedHashSet<>(range.getValue());
                if (pending != null && pending.containsKey(range.getKey()))
                    replicas.addAll(pending.get(range.getKey()));

                boolean ok = replicas.size() >= floor;
                ObjectNode d = Harness.details();
                d.put("node", n.host);
                d.put("range", String.valueOf(range.getKey()));
                d.put("write_replica_count", replicas.size());
                d.put("required_floor", floor);
                d.put("configured_rf", Harness.PROBE_RF);
                d.put("live_nodes", liveNodes);
                d.put("sequences_in_flight", sequencesInFlight);
                Assert.always(ok, RF_PRESERVED, d);
                if (!ok)
                    break;
            }

            // --- ring coverage ---
            ringCovered(n, ranges, sequencesInFlight);
        }
        record(read);
    }

    /**
     * Ring contiguity for one node's view. Checks that the ranges abut and wrap exactly once;
     * checking gaps without checking the wrap count would miss a ring covered twice.
     *
     * <p>Records whether sequences were in flight (evaluation refinement R5): evaluated on a
     * settled ring this is largely a unit test, and its Antithesis value comes entirely from
     * evaluations during interrupted concurrent movements.
     */
    private void ringCovered(Node node,
                             Map<List<String>, List<String>> ranges,
                             int sequencesInFlight)
    {
        List<long[]> intervals = new ArrayList<>();
        for (List<String> bounds : ranges.keySet())
        {
            if (bounds == null || bounds.size() != 2)
                return; // unexpected shape; do not invent a verdict
            try
            {
                intervals.add(new long[]{ Long.parseLong(bounds.get(0)),
                                          Long.parseLong(bounds.get(1)) });
            }
            catch (NumberFormatException e)
            {
                return; // non-Murmur3 tokens; contiguity arithmetic does not apply
            }
        }
        if (intervals.isEmpty())
            return;

        intervals.sort(Comparator.comparingLong(iv -> iv[0]));
        int wraps = 0;
        int gaps = 0;
        for (int i = 0; i < intervals.size(); i++)
        {
            long[] current = intervals.get(i);
            long[] next = intervals.get((i + 1) % intervals.size());
            if (current[1] != next[0])
            {
                // The one legitimate discontinuity is the wrap point, where the last range's end
                // meets the first range's start through the minimum token.
                if (i == intervals.size() - 1)
                    wraps++;
                else
                    gaps++;
            }
        }

        boolean ok = gaps == 0;
        if (sequencesInFlight > 0)
            harness.state.bump("ring_checks_during_churn");
        else
            harness.state.bump("ring_checks_settled");

        ObjectNode d = Harness.details();
        d.put("node", node.host);
        d.put("range_count", intervals.size());
        d.put("gaps", gaps);
        d.put("wrap_discontinuities", wraps);
        d.put("sequences_in_flight", sequencesInFlight);
        d.put("checks_during_churn", harness.state.counter("ring_checks_during_churn"));
        d.put("checks_settled", harness.state.counter("ring_checks_settled"));
        Assert.always(ok, RING_COVERED, d);
    }

    /**
     * {@code d-peers-table-matches-directory} -- derived peer state agrees with metadata.
     *
     * <p>Scoped by sequence activity rather than by enumerating {@code NodeState} values: nodes
     * with an active multi-step operation are skipped, which is robust to not knowing the exact
     * state filter the peers-table writer applies. The historical bugs all manifested as drift
     * persisting *after* a movement, so the exclusion costs little.
     */
    public void peersMatchDirectory()
    {
        int read = 0;
        for (Node n : harness.nodes)
        {
            Session s = n.pinnedSession();
            Map<Long, Map<String, String>> dir = n.dumpDirectory(false);
            if (s == null || dir == null || dir.isEmpty())
                continue;

            // Identify the local node by host_id from system.local. A node never lists itself in
            // system.peers_v2, so its directory entry must be excluded from the comparison -- and
            // excluding it by *address* is unreliable, because the directory stores the self entry's
            // broadcast_address as a bare IP while n.host is a container hostname, so a
            // hostname-vs-IP mismatch left self in the set and produced a false positive on the
            // first local run. host_id is the stable identity that both tables agree on.
            String selfHostId = null;
            try
            {
                Row local = s.execute("SELECT host_id FROM system.local").one();
                if (local != null && local.getUUID("host_id") != null)
                    selfHostId = local.getUUID("host_id").toString();
            }
            catch (RuntimeException e)
            {
                Log.debug(n.host + ": system.local read failed: " + e);
                continue; // cannot identify self; skip rather than risk a false positive
            }
            read++;

            // Directory rows in a settled state, indexed by broadcast address, excluding self.
            Map<String, Map<String, String>> settled = new LinkedHashMap<>();
            for (Map<String, String> row : dir.values())
            {
                String mso = row.get("multi_step_operation");
                boolean inFlight = mso != null && !mso.isEmpty() && !"{}".equals(mso.trim());
                String state = String.valueOf(row.get("state"));
                String address = row.get("broadcast_address");
                String hostId = row.get("host_id");
                if (inFlight || address == null)
                    continue;
                if (selfHostId != null && selfHostId.equalsIgnoreCase(String.valueOf(hostId)))
                    continue; // self is legitimately absent from peers_v2
                // LEFT nodes persist in the directory until `nodetool cms unregister`, and
                // REGISTERED/BOOTSTRAPPING nodes have no peers_v2 row yet. Only JOINED-equivalent
                // states are comparable.
                if (!"JOINED".equalsIgnoreCase(state) && !"NORMAL".equalsIgnoreCase(state))
                    continue;
                settled.put(normalizeAddress(address), row);
            }

            Set<String> peerAddresses = new HashSet<>();
            try
            {
                for (Row row : s.execute("SELECT peer, host_id FROM system.peers_v2"))
                    peerAddresses.add(normalizeAddress(String.valueOf(row.getInet("peer"))));
            }
            catch (RuntimeException e)
            {
                Log.debug(n.host + ": peers_v2 read failed: " + e);
                continue;
            }

            Set<String> expected = new LinkedHashSet<>(settled.keySet());

            Set<String> missingFromPeers = new LinkedHashSet<>(expected);
            missingFromPeers.removeAll(peerAddresses);

            boolean ok = missingFromPeers.isEmpty();
            ObjectNode d = Harness.details();
            d.put("node", n.host);
            d.put("settled_directory_entries", settled.size());
            d.put("peers_v2_rows", peerAddresses.size());
            ArrayNode missing = d.putArray("in_directory_but_not_peers_v2");
            missingFromPeers.forEach(missing::add);
            Assert.always(ok, PEERS_MATCH_DIRECTORY, d);
        }
        record(read);
    }

    private static String normalizeAddress(String raw)
    {
        if (raw == null)
            return "";
        String s = raw.startsWith("/") ? raw.substring(1) : raw;
        int slash = s.indexOf('/');
        if (slash >= 0)
            s = s.substring(slash + 1);
        int colon = s.lastIndexOf(':');
        if (colon > 0 && s.indexOf(':') == colon) // IPv4 host:port only
            s = s.substring(0, colon);
        return s;
    }

    /** Address-valued directory fields whose rendering differs between self- and peer-views. */
    private static final Set<String> ADDRESS_FIELDS =
        new LinkedHashSet<>(java.util.Arrays.asList("broadcast_address", "native_address",
                                                    "local_address"));

    /**
     * Directory fields excluded from the cross-node equality entirely, because their *rendering* is
     * not a stable function of {@code ClusterMetadata} even at a fixed epoch.
     *
     * <p>{@code multi_step_operation} is the offender found by the first Antithesis run: its value is
     * {@code mso.status()}, which renders an {@code AffectedRangesImpl{map={ReplicationParams -> Set<Range>}}}
     * — an unordered {@code HashMap}/{@code Set}. Two nodes at the same epoch hold the identical
     * operation (same 48 ranges, same RF set) but render the map in different iteration order, so a
     * byte comparison reports divergence that does not exist. Triage of run
     * {@code 8bf9b2c6…-59-13} confirmed: masking this field made the two directories exactly equal.
     * In-progress sequences are checked structurally by {@code b-locked-ranges-match-sequences} and
     * the sequence-drain checks; they do not need a fragile string equality here. The field is still
     * emitted in {@code r-concurrent-multistep-operations}' details for triage.
     */
    private static final Set<String> EXCLUDED_FIELDS =
        new LinkedHashSet<>(java.util.Arrays.asList("multi_step_operation"));

    /**
     * Renders a directory dump into a form comparable across nodes.
     *
     * <p>Two rendering artifacts must be neutralised, both discovered by real runs rather than by
     * reasoning, and both of the same shape (comparing a rendered string that is not canonical):
     *
     * <ul>
     *   <li><b>Addresses</b> ({@link #ADDRESS_FIELDS}): a node renders its own entry as
     *       {@code hostname/ip} but peers render it {@code /ip} — an {@code InetAddress.toString()}
     *       artifact. Reduced to the bare IP via {@link #normalizeAddress}; a genuine IP divergence
     *       still shows.
     *   <li><b>{@code multi_step_operation}</b> ({@link #EXCLUDED_FIELDS}): {@code mso.status()}
     *       renders unordered maps/sets, so equal metadata renders in different order. Excluded
     *       from the comparison entirely — see that field's note.
     * </ul>
     *
     * Keys are sorted so iteration order of the fields themselves cannot cause a mismatch.
     */
    static String canonicalDirectory(Map<Long, Map<String, String>> dir)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Map<String, String>> node : new TreeMap<>(dir).entrySet())
        {
            sb.append(node.getKey()).append('=').append('{');
            for (Map.Entry<String, String> field : new TreeMap<>(node.getValue()).entrySet())
            {
                if (EXCLUDED_FIELDS.contains(field.getKey()))
                    continue;
                String value = field.getValue();
                if (ADDRESS_FIELDS.contains(field.getKey()))
                    value = normalizeAddress(value);
                sb.append(field.getKey()).append('=').append(value).append(", ");
            }
            sb.append("} ");
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------------------------------
    // Category E and R
    // -------------------------------------------------------------------------------------------

    /**
     * {@code e-cluster-serves-requests-during-churn} -- the cluster is not continuously unusable.
     *
     * <p>Deliberately does not assert that the read returns the write. That is a correctness claim
     * this property is not making; a weak version of it here would be a poor substitute for the
     * linearizability workload recorded as bias B1 in {@code evaluation/synthesis.md}.
     */
    public void availableDuringChurn()
    {
        Session s = harness.session();
        boolean wrote = false, readBack = false;
        if (s != null)
        {
            String key = "probe-" + harness.random.nextInt(1_000_000);
            try
            {
                s.execute(String.format(
                    "INSERT INTO %s.%s (id, payload) VALUES ('%s', 'v')",
                    Harness.PROBE_KEYSPACE, Harness.PROBE_TABLE, key));
                wrote = true;
            }
            catch (RuntimeException e)
            {
                Log.debug("probe write failed: " + e);
            }
            try
            {
                s.execute(String.format("SELECT id FROM %s.%s WHERE id = '%s'",
                                        Harness.PROBE_KEYSPACE, Harness.PROBE_TABLE, key));
                readBack = true;
            }
            catch (RuntimeException e)
            {
                Log.debug("probe read failed: " + e);
            }
        }

        boolean ok = wrote && readBack;
        harness.state.bump(ok ? "probe_cycles_ok" : "probe_cycles_failed");

        ObjectNode d = Harness.details();
        d.put("write_succeeded", wrote);
        d.put("read_succeeded", readBack);
        d.put("cycles_ok", harness.state.counter("probe_cycles_ok"));
        d.put("cycles_failed", harness.state.counter("probe_cycles_failed"));
        Assert.sometimes(ok, AVAILABLE_DURING_CHURN, d);
    }

    /**
     * Observes in-flight multi-step-operation concurrency for context.
     *
     * <p>Does NOT assert: the {@code r-concurrent-multistep-operations} assertion lives in
     * {@code Actions.launchConcurrentMovements()}, which controls the timing tightly enough to catch
     * the overlap. Run 8bf9b2c6...-59-13 showed the periodic {@code anytime} sweep almost never
     * sampled while two sequences coexisted (ex=0), so asserting here just produced a permanent
     * red. This still records the peak (used as churn context for {@code d-ring-fully-owned} and in
     * the run summary).
     *
     * @return the highest number of distinct in-flight sequences seen on any node
     */
    public int concurrentSequences()
    {
        int max = 0;
        int read = 0;
        for (Node n : harness.nodes)
        {
            Map<Long, Map<String, String>> dir = n.dumpDirectory(false);
            if (dir == null || dir.isEmpty())
                continue;
            read++;
            // Count directory entries (each a distinct owning node id) that carry an mso, NOT
            // distinct mso strings -- two same-phase sequences render identically and a string set
            // would collapse them to one (see Actions.currentInProgressSequences).
            int sequences = 0;
            for (Map<String, String> row : dir.values())
            {
                String mso = row.get("multi_step_operation");
                if (mso != null && !mso.isEmpty() && !"{}".equals(mso.trim()))
                    sequences++;
            }
            max = Math.max(max, sequences);
        }
        record(read);

        if (max > harness.state.counter("max_concurrent_sequences"))
            harness.state.counters.put("max_concurrent_sequences", (long) max);
        return max;
    }

    /**
     * {@code h-all-nodes-compared} -- a harness self-check.
     *
     * <p>The total is a configured constant, never the number of nodes currently discoverable:
     * deriving it from discovery would make the property self-satisfying, since a workload that can
     * only see three nodes would compare 3 == 3 and pass.
     */
    public void allNodesCompared()
    {
        int total = harness.nodes.size();
        boolean complete = maxNodesCompared >= total;
        harness.state.bump(complete ? "cycles_all_nodes" : "cycles_partial_nodes");

        ObjectNode d = Harness.details();
        d.put("nodes_compared", maxNodesCompared);
        d.put("nodes_configured", total);
        d.put("cycles_complete", harness.state.counter("cycles_all_nodes"));
        d.put("cycles_partial", harness.state.counter("cycles_partial_nodes"));
        Assert.sometimes(complete, ALL_NODES_COMPARED, d);
    }

    // -------------------------------------------------------------------------------------------
    // Quiet-period checks (eventually_)
    // -------------------------------------------------------------------------------------------

    // Terminal operation modes: a node in one of these has left the cluster and will never advance
    // its epoch again. Its container may still be running and answering JMX (decommission does not
    // stop the process), so it must be excluded from convergence -- otherwise a deliberately
    // decommissioned node, frozen at its final epoch, is read as a live node that failed to
    // converge. This was the third false positive the local run produced.
    private static final Set<String> LEFT_MODES =
        new LinkedHashSet<>(java.util.Arrays.asList(
            "DECOMMISSIONED", "LEFT", "DRAINED", "DRAINING", "DECOMMISSION_FAILED"));

    private boolean hasLeftCluster(Node n)
    {
        String mode = n.operationMode();
        return mode != null && LEFT_MODES.contains(mode);
    }

    /**
     * {@code e-cluster-converges-after-faults} -- all live *member* nodes reach one identical epoch.
     *
     * <p>Progress-based rather than deadline-based: LOCAL_PENDING distinguishes "still catching up"
     * from "genuinely stuck", so a slow but progressing catch-up is not reported as a violation.
     * There is no published end-to-end convergence bound to use instead.
     *
     * <p>"Live member" excludes nodes that have left the cluster (DECOMMISSIONED / LEFT / DRAINED).
     * Such a node stops advancing its epoch by design, yet its process keeps answering JMX, so
     * counting it would report a permanent non-convergence that is actually correct behaviour. This
     * is the same "answers JMX != is a cluster member" distinction as h-all-nodes-compared.
     */
    public void convergence(long maxWaitMillis)
    {
        long deadline = System.currentTimeMillis() + maxWaitMillis;
        Map<String, Map<String, String>> members = null;
        Set<Long> epochs = null;
        long lastPendingTotal = Long.MAX_VALUE;
        int stalledPolls = 0;

        while (System.currentTimeMillis() < deadline)
        {
            Map<String, Map<String, String>> snapshot = cmsSnapshot();
            members = new LinkedHashMap<>();
            epochs = new LinkedHashSet<>();
            long pendingTotal = 0;
            for (Map.Entry<String, Map<String, String>> e : snapshot.entrySet())
            {
                if (hasLeftCluster(harness.node(e.getKey())))
                    continue; // decommissioned/left node: correctly frozen, not a convergence target
                members.put(e.getKey(), e.getValue());
                epochs.add(epochOf(e.getValue()));
                try
                {
                    pendingTotal += Long.parseLong(e.getValue().getOrDefault("LOCAL_PENDING", "0"));
                }
                catch (NumberFormatException ignored2)
                {
                }
            }

            if (!epochs.isEmpty() && epochs.size() == 1 && pendingTotal == 0)
                break;

            // No forward motion across several polls means waiting longer will not help.
            if (pendingTotal >= lastPendingTotal)
                stalledPolls++;
            else
                stalledPolls = 0;
            lastPendingTotal = pendingTotal;
            if (stalledPolls >= 12)
            {
                Log.warn("convergence appears stalled (pending=" + pendingTotal + ")");
                break;
            }

            sleep(5_000);
        }

        if (members == null || members.isEmpty())
        {
            Log.warn("no member node answered during convergence check; not asserting");
            return;
        }

        // Directory agreement too, over the same member set: equal epochs with unequal metadata is a
        // log-agreement failure surfacing here, and catching it in the quiet period gives a cleaner
        // signal. Uses canonicalDirectory so the address self/peer rendering artifact does not count.
        Set<String> directories = new LinkedHashSet<>();
        for (String host : members.keySet())
        {
            Node n = harness.node(host);
            Map<Long, Map<String, String>> dir = n.dumpDirectory(false);
            if (dir != null && !dir.isEmpty())
                directories.add(canonicalDirectory(dir));
        }

        boolean ok = epochs != null && epochs.size() == 1 && directories.size() <= 1;

        ObjectNode d = Harness.details();
        d.put("member_nodes_answered", members.size());
        d.put("nodes_configured", harness.nodes.size());
        d.put("distinct_epochs", epochs == null ? -1 : epochs.size());
        d.put("distinct_directories", directories.size());
        ObjectNode perNode = d.putObject("per_node");
        for (Map.Entry<String, Map<String, String>> e : members.entrySet())
        {
            ObjectNode row = perNode.putObject(e.getKey());
            row.put("epoch", epochOf(e.getValue()));
            row.put("local_pending", e.getValue().get("LOCAL_PENDING"));
            row.put("service_state", e.getValue().get("SERVICE_STATE"));
        }
        Assert.always(ok, CONVERGED, d);
    }

    /**
     * {@code b-sequence-resumable-after-crash} -- sequences drain once faults stop.
     *
     * <p>Sound only because the caller restarts every node it stopped first: CEP-21 is explicit
     * that failure detection must never cancel a sequence, so a sequence owned by a permanently
     * absent node is *correctly* stuck and asserting it drains would assert the opposite of the
     * design.
     */
    public void sequencesDrained(long maxWaitMillis)
    {
        long deadline = System.currentTimeMillis() + maxWaitMillis;
        int inFlight = -1;
        ObjectNode remaining = Harness.details();

        while (System.currentTimeMillis() < deadline)
        {
            inFlight = 0;
            remaining = Harness.details();
            for (Node n : harness.nodes)
            {
                Map<Long, Map<String, String>> dir = n.dumpDirectory(false);
                if (dir == null)
                    continue;
                for (Map.Entry<Long, Map<String, String>> e : dir.entrySet())
                {
                    String mso = e.getValue().get("multi_step_operation");
                    if (mso != null && !mso.isEmpty() && !"{}".equals(mso.trim()))
                    {
                        inFlight++;
                        remaining.put(n.host + "/node" + e.getKey(), mso);
                    }
                }
            }
            if (inFlight == 0)
                break;
            sleep(5_000);
        }

        if (inFlight < 0)
            return; // nothing readable; no verdict

        ObjectNode d = Harness.details();
        d.put("sequences_remaining", inFlight);
        d.set("remaining_detail", remaining);
        Assert.always(inFlight == 0, SEQUENCES_DRAINED, d);
    }

    /**
     * {@code e-cms-accepts-commits-after-recovery} -- the CMS can still commit.
     *
     * <p>Uses a real DDL round trip, and a uniquely named table per attempt so a retry cannot be
     * satisfied by a previously-committed change.
     */
    public void cmsAcceptsCommit(long maxWaitMillis)
    {
        long deadline = System.currentTimeMillis() + maxWaitMillis;
        boolean committed = false;
        long attempts = 0;
        String lastError = "";
        String table = "recovery_probe_" + Math.abs(harness.random.nextLong());

        while (System.currentTimeMillis() < deadline && !committed)
        {
            attempts++;
            Session s = harness.session();
            if (s != null)
            {
                try
                {
                    s.execute(String.format(
                        "CREATE TABLE %s.%s (id text PRIMARY KEY, payload text)",
                        Harness.PROBE_KEYSPACE, table));
                    committed = true;
                    break;
                }
                catch (RuntimeException e)
                {
                    lastError = String.valueOf(e);
                }
            }
            sleep(5_000);
        }

        ObjectNode d = Harness.details();
        d.put("committed", committed);
        d.put("attempts", attempts);
        d.put("table", table);
        d.put("last_error", lastError);
        ObjectNode perNode = d.putObject("per_node");
        for (Map.Entry<String, Map<String, String>> e : cmsSnapshot().entrySet())
        {
            ObjectNode row = perNode.putObject(e.getKey());
            row.put("epoch", epochOf(e.getValue()));
            row.put("needs_reconfiguration", e.getValue().get("NEEDS_RECONFIGURATION"));
            row.put("service_state", e.getValue().get("SERVICE_STATE"));
            row.put("commits_paused", e.getValue().get("COMMITS_PAUSED"));
        }
        Assert.always(committed, CMS_ACCEPTS_COMMIT, d);
    }

    // -------------------------------------------------------------------------------------------

    private static boolean eq(Object a, Object b)
    {
        return a == null ? b == null : a.equals(b);
    }

    private static ObjectNode jsonOf(Map<String, String> map)
    {
        ObjectNode node = Harness.details();
        map.forEach((k, v) -> node.put(k, v));
        return node;
    }

    static void sleep(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }
}
