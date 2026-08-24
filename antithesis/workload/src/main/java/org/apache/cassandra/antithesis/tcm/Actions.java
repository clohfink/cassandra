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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;

import com.antithesis.sdk.Assert;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.apache.cassandra.antithesis.tcm.Harness.Log;
import org.apache.cassandra.antithesis.tcm.Harness.Node;

/**
 * Workload actions: the things that make TCM change state.
 *
 * <p>Two policies apply throughout, both from the evaluation
 * ({@code scratchbook/evaluation/synthesis.md}):
 *
 * <ul>
 *   <li><b>R9 -- decline, do not provoke.</b> The workload refuses to submit an operation that
 *       would drop the ring below RF or the CMS below its configured RF, and logs the refusal.
 *       Those operations are *supposed* to be rejected by TCM; forcing them would make
 *       {@code b-replication-factor-never-under} and {@code c-cms-membership-never-empty} fire on
 *       correct behaviour, and the usual response to a noisy P0 assertion is to weaken it.
 *   <li><b>Range-selection variety.</b> {@code r-concurrent-multistep-operations} wants disjoint
 *       concurrent movements (so they are admitted); {@code r-commit-rejected} wants overlapping
 *       ones (so they are rejected). A workload that always does one starves the other, so the
 *       choice is randomised.
 * </ul>
 */
public final class Actions
{
    private static final String PREPARED_NOT_STALE =
        "a prepared statement never returns results shaped by a stale table definition";
    private static final String COORDINATOR_BEHIND =
        "a replica rejected a request because the coordinator was behind";

    private final Harness harness;

    public Actions(Harness harness)
    {
        this.harness = harness;
    }

    // -------------------------------------------------------------------------------------------
    // Setup
    // -------------------------------------------------------------------------------------------

    /**
     * Grows the CMS to {@code rf} members, and waits for it.
     *
     * <p>This is setup, not churn, and it is <b>required</b> rather than a nicety. A freshly
     * initialised cluster has a CMS of exactly one member with
     * {@code ReplicationParams{class=MetaStrategy, datacenter1=1}} -- observed directly during
     * bring-up. That single member is a single point of failure for *all* metadata: with it briefly
     * unavailable, `CREATE KEYSPACE` fails with
     * {@code GivingUpException: Could not succeed sending TCM_COMMIT_REQ to
     * CandidateIterator{candidates=[<the one member>]}}, which is exactly how this was found.
     *
     * <p>It also decides whether two properties mean anything: with one member, every CMS quorum is
     * that member, so {@code c-cms-reconfiguration-quorum-overlap} is trivially satisfied and
     * {@code c-cms-membership-never-empty} has no interesting failure mode. CEP-21's worked example
     * needs at least three.
     *
     * @return true if the CMS reached rf members
     */
    public boolean ensureCmsReplicationFactor(int rf, long maxWaitMillis)
    {
        Node via = null;
        for (Node n : harness.nodes)
        {
            if (n.describeCMS() != null)
            {
                via = n;
                break;
            }
        }
        if (via == null)
        {
            Log.warn("no reachable node to reconfigure the CMS");
            return false;
        }

        int current = cmsMemberCount(via);
        if (current >= rf)
        {
            Log.info("CMS already has " + current + " members");
            return true;
        }

        // Clamp to what is actually satisfiable. CMSPlacementStrategy.reconfigure() rejects an RF
        // exceeding the number of endpoints in the datacenter with "There are not enough nodes in
        // <dc> datacenter to satisfy replication factor" -- and only nodes that have JOINED the ring
        // count, so the two join_ring=false spares do not. Requesting an impossible RF is the same
        // mistake the R9 policy avoids elsewhere: it produces a rejection, not a reconfiguration.
        int eligible = ringNodes().size();
        int target = Math.min(rf, eligible);
        if (target <= current)
        {
            Log.warn("cannot grow the CMS: " + eligible + " node(s) in the ring, "
                     + current + " already CMS member(s), requested rf=" + rf
                     + ". Leaving the CMS at " + current + ".");
            Log.warn("  Consequence: with a " + current + "-member CMS, "
                     + "c-cms-reconfiguration-quorum-overlap is trivially satisfied and "
                     + "c-cms-membership-never-empty has no interesting failure mode. "
                     + "r-cms-reconfiguration-observed will report that reconfiguration never ran.");
            return false;
        }

        Log.info("growing CMS from " + current + " to " + target + " members via " + via.host
                 + " (requested " + rf + ", " + eligible + " nodes in the ring)");
        via.reconfigureCMS(target);

        long deadline = System.currentTimeMillis() + maxWaitMillis;
        while (System.currentTimeMillis() < deadline)
        {
            int now = cmsMemberCount(via);
            if (now >= target)
            {
                Log.info("CMS reconfiguration complete: " + now + " members");
                return true;
            }
            Map<String, List<String>> status = via.reconfigureCMSStatus();
            Log.info("waiting for CMS reconfiguration: members=" + now + "/" + target
                     + " status=" + (status == null ? "unavailable" : status));
            Checks.sleep(5_000);
        }
        Log.warn("CMS did not reach " + target + " members before the deadline");
        return false;
    }

    /** Counts entries in describeCMS's MEMBERS string, which is a comma-joined list of tuples. */
    private int cmsMemberCount(Node node)
    {
        Map<String, String> cms = node.describeCMS();
        if (cms == null)
            return -1;
        String members = cms.getOrDefault("MEMBERS", "").trim();
        if (members.isEmpty())
            return 0;
        // Format: "(nodeid=1,address=/10.0.0.1:7000),(nodeid=2,address=...)"
        int count = 0;
        for (int i = 0; i < members.length(); i++)
        {
            if (members.charAt(i) == '(')
                count++;
        }
        return count;
    }

    /**
     * Creates the probe keyspace and table. Run from the {@code first_} command, which Antithesis
     * schedules after {@code setup_complete} and before any driver, with no faults active.
     */
    public void createProbeSchema()
    {
        Session s = harness.session();
        if (s == null)
            throw new IllegalStateException("no CQL session available to create probe schema");

        s.execute(String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = "
            + "{'class': 'SimpleStrategy', 'replication_factor': %d}",
            Harness.PROBE_KEYSPACE, Harness.PROBE_RF));
        s.execute(String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (id text PRIMARY KEY, payload text)",
            Harness.PROBE_KEYSPACE, Harness.PROBE_TABLE));
        // Dedicated table for d-prepared-statement-not-stale so schema churn elsewhere cannot
        // interfere with its column expectations.
        s.execute(String.format(
            "CREATE TABLE IF NOT EXISTS %s.prepared_probe (id text PRIMARY KEY, c0 text)",
            Harness.PROBE_KEYSPACE));
        Log.info("probe schema ready in keyspace " + Harness.PROBE_KEYSPACE);
    }

    // -------------------------------------------------------------------------------------------
    // Schema churn -- the metadata-commit workhorse
    // -------------------------------------------------------------------------------------------

    /**
     * Commits uniquely-tagged DDL and records the outcome in the ledger that
     * {@code c-commit-survives-cms-membership-change} checks.
     *
     * <p>The tag is embedded in the table name so it appears in the log entry's rendered
     * transformation. {@code CustomTransformation} would be lighter but has no confirmed
     * client-reachable submission path (see that property's evidence file); DDL is heavier and is
     * also the path operators actually use.
     *
     * <p>A timeout is recorded as {@code unknown}, never as failed. Collapsing unknown into failed
     * is what turns exactly-once checking into a false-positive generator under partition.
     */
    public void schemaChurn(int operations)
    {
        for (int i = 0; i < operations; i++)
        {
            Session s = harness.session();
            if (s == null)
            {
                Log.debug("no session for schema churn; skipping");
                return;
            }

            String tag = "tcmtag_" + Long.toHexString(harness.random.nextLong() & 0xffffffffL)
                         + "_" + i;
            String cql = String.format(
                "CREATE TABLE %s.%s (id text PRIMARY KEY, payload text)",
                Harness.PROBE_KEYSPACE, tag);

            harness.state.tagLedger.put(tag, "unknown");
            harness.state.save(); // must be durable *before* the attempt

            try
            {
                s.execute(cql);
                harness.state.tagLedger.put(tag, "acked");
                harness.state.bump("ddl_acked");
            }
            catch (com.datastax.driver.core.exceptions.AlreadyExistsException e)
            {
                // NOT a rejection: on a CREATE TABLE, AlreadyExists means the transformation already
                // committed and the client is seeing a retry (its first ack was lost, the driver
                // resent). The table is present exactly once, so this is "acked". Triage of run
                // 8bf9b2c6...-59-13 showed this misclassified as "rejected", which then tripped the
                // exactly-once check ("a rejected transformation must appear 0 times") on a table
                // that had legitimately committed. Only reachable under faults (lost acks), which is
                // why local no-fault runs never hit it.
                harness.state.tagLedger.put(tag, "acked");
                harness.state.bump("ddl_acked_via_already_exists");
                Log.debug("DDL AlreadyExists (committed, ack lost then retried) for " + tag);
            }
            catch (com.datastax.driver.core.exceptions.InvalidQueryException e)
            {
                // Recorded as UNKNOWN, not "rejected". A client-side InvalidQueryException does not
                // prove the CMS declined to persist the transformation: under faults a CREATE TABLE
                // can commit at the CMS and still surface a client error when the ack is lost or the
                // coordinator is behind. Run 32d96d63...-59-13 showed exactly this -- an InvalidQuery
                // tag present once -- and mislabelling it "rejected" (=> must be absent) produced a
                // false exactly-once failure. The authoritative reject signal is the SUT-side
                // "a transformation was rejected by the CMS" assertion, not the driver exception.
                harness.state.tagLedger.put(tag, "unknown");
                harness.state.bump("ddl_invalidquery_unknown");
                Log.debug("DDL InvalidQuery (outcome unknown -- may have committed) for " + tag + ": " + e);
            }
            catch (RuntimeException e)
            {
                // Timeout, unavailable, connection loss: outcome genuinely unknown.
                harness.state.bump("ddl_unknown");
                Log.debug("DDL outcome unknown for " + tag + ": " + e);
            }
            harness.state.save();

            // Bound the ledger: it is compared against the whole log on every check cycle, and an
            // unbounded ledger would make the checker's cost grow without bound over a long run.
            if (harness.state.tagLedger.size() > 200)
            {
                List<String> oldest = new ArrayList<>(harness.state.tagLedger.keySet());
                for (int k = 0; k < oldest.size() - 200; k++)
                    harness.state.tagLedger.remove(oldest.get(k));
            }
        }
    }

    /**
     * {@code d-prepared-statement-not-stale} -- prepare, alter, re-execute on the same node.
     *
     * <p>Waits for the node's own epoch to pass the alter before re-executing, so the check does
     * not assume any propagation delay: it waits for the node to admit it has the change, then
     * verifies the node's derived state agrees.
     */
    public void preparedStatementStaleness()
    {
        Node node = harness.randomNode();
        Session s = node.pinnedSession();
        if (s == null)
        {
            Log.debug(node.host + ": no pinned session for prepared-statement check");
            return;
        }

        String column = "c" + (1 + harness.random.nextInt(1_000_000));
        String key = "pk-" + harness.random.nextInt(1_000_000);
        PreparedStatement prepared;
        long epochBefore;
        try
        {
            prepared = s.prepare(String.format(
                "SELECT * FROM %s.prepared_probe WHERE id = ?", Harness.PROBE_KEYSPACE));
            s.execute(String.format(
                "INSERT INTO %s.prepared_probe (id, c0) VALUES ('%s', 'before')",
                Harness.PROBE_KEYSPACE, key));
            Map<String, String> cms = node.describeCMS();
            epochBefore = cms == null ? -1 : Long.parseLong(cms.getOrDefault("EPOCH", "-1"));

            s.execute(String.format("ALTER TABLE %s.prepared_probe ADD %s text",
                                    Harness.PROBE_KEYSPACE, column));
        }
        catch (RuntimeException e)
        {
            Log.debug(node.host + ": prepared-statement setup failed: " + e);
            return;
        }

        // Wait for this node to acknowledge an epoch beyond the one it had before the ALTER.
        long deadline = System.currentTimeMillis() + 60_000;
        long epochAfter = epochBefore;
        while (System.currentTimeMillis() < deadline)
        {
            Map<String, String> cms = node.describeCMS();
            if (cms != null)
            {
                try
                {
                    epochAfter = Long.parseLong(cms.getOrDefault("EPOCH", "-1"));
                }
                catch (NumberFormatException ignored)
                {
                }
            }
            if (epochAfter > epochBefore)
                break;
            Checks.sleep(1_000);
        }
        if (epochAfter <= epochBefore)
        {
            Log.debug(node.host + ": epoch did not advance past the ALTER; not asserting");
            return;
        }

        boolean sawNewColumn = false;
        boolean failedCleanly = false;
        String error = "";
        try
        {
            ResultSet rs = s.execute(prepared.bind(key));
            sawNewColumn = rs.getColumnDefinitions().contains(column);
            // Consume so any deferred server error surfaces here rather than being swallowed.
            for (Row ignored : rs)
            {
                break;
            }
        }
        catch (RuntimeException e)
        {
            failedCleanly = true;
            error = String.valueOf(e);
        }

        // Either outcome is acceptable: the new shape, or a clean failure. Returning the OLD shape
        // after the node's own epoch has advanced past the change is the violation.
        boolean ok = sawNewColumn || failedCleanly;

        ObjectNode d = Harness.details();
        d.put("node", node.host);
        d.put("added_column", column);
        d.put("epoch_before_alter", epochBefore);
        d.put("epoch_after_alter", epochAfter);
        d.put("saw_new_column", sawNewColumn);
        d.put("failed_cleanly", failedCleanly);
        d.put("error", error);
        // The driver may transparently re-prepare on an unprepared error, which makes "server served
        // the new shape" and "server rejected, driver recovered" indistinguishable here. Both are
        // acceptable, but the distinction is recorded so a run can tell whether invalidation is
        // actually working -- see this property's evidence file.
        d.put("note_driver_may_reprepare", true);
        Assert.always(ok, PREPARED_NOT_STALE, d);
    }

    // Replica-side meters marked immediately before a CoordinatorBehindException is thrown, from
    // ReadCommandVerbHandler / AbstractMutationVerbHandler / ReadCommand / PartitionUpdate. Reading
    // these is the ONLY reliable observation: StorageProxy catches CoordinatorBehindException on the
    // coordinator and *retries* (marking ClientRequest RetryCoordinatorBehind), so the client never
    // sees an error and system_views.exceptions does not capture it. Confirmed by reading the source
    // during triage of run 8bf9b2c6...-59-13, where the exception-text approach reported ex=0.
    private static final String CB_SCHEMA_METER =
        "org.apache.cassandra.metrics:type=TCM,name=CoordinatorBehindSchema";
    private static final String CB_PLACEMENTS_METER =
        "org.apache.cassandra.metrics:type=TCM,name=CoordinatorBehindPlacements";

    /**
     * {@code r-coordinator-behind-rejection} -- detect that a replica told a coordinator it was
     * behind by a *materially relevant* epoch.
     *
     * <p>Two halves, both reworked after run 8bf9b2c6...-59-13 (which never fired, ex=0):
     *
     * <ol>
     *   <li><b>Trigger.</b> Churn schema on the probe table (bumps its schema last-modified epoch),
     *       then drive a write *and* a read through EVERY node's pinned single-host session so each
     *       node in turn coordinates a materially-relevant request. Under fault injection some node
     *       is behind; when it coordinates a request touching the just-changed table/range, the
     *       replica throws and marks the meter. Writes matter because
     *       {@code AbstractMutationVerbHandler} is the richest throw site (placements + schema).
     *   <li><b>Observe.</b> Read the replica-side meters' cumulative {@code Count} across all
     *       reachable nodes. Any node with a nonzero count means the condition occurred. This
     *       replaces the earlier {@code system_views.exceptions} scan, which could never work because
     *       the exception is caught+retried on the coordinator and never surfaced.
     * </ol>
     */
    public void coordinatorBehindProbe()
    {
        Session balanced = harness.session();
        if (balanced != null)
        {
            try
            {
                balanced.execute(String.format("ALTER TABLE %s.%s WITH comment = 'cb-churn-%d'",
                                               Harness.PROBE_KEYSPACE, Harness.PROBE_TABLE,
                                               harness.random.nextInt(1_000_000)));
            }
            catch (RuntimeException e)
            {
                Log.debug("coordinator-behind schema churn failed: " + e);
            }
        }

        // Trigger: coordinate a materially-relevant write+read through each node, so a lagging node
        // gets a chance to be the coordinator that a healthy replica rejects.
        String key = "cb-" + harness.random.nextInt(1_000_000);
        for (Node n : harness.nodes)
        {
            Session pinned = n.pinnedSession();
            if (pinned == null)
                continue;
            try
            {
                pinned.execute(String.format(
                    "INSERT INTO %s.%s (id, payload) VALUES ('%s', 'cb')",
                    Harness.PROBE_KEYSPACE, Harness.PROBE_TABLE, key));
            }
            catch (RuntimeException e)
            {
                Log.debug(n.host + ": pinned write failed (may be catching up): " + e);
            }
            try
            {
                pinned.execute(String.format("SELECT id FROM %s.%s WHERE id = '%s'",
                                             Harness.PROBE_KEYSPACE, Harness.PROBE_TABLE, key));
            }
            catch (RuntimeException e)
            {
                Log.debug(n.host + ": pinned read failed (may be catching up): " + e);
            }
        }

        // Observe: cumulative replica-side meter counts.
        long schemaTotal = 0, placementsTotal = 0;
        int read = 0;
        for (Node n : harness.nodes)
        {
            long sc = n.meterCount(CB_SCHEMA_METER);
            long pl = n.meterCount(CB_PLACEMENTS_METER);
            if (sc < 0 && pl < 0)
                continue; // node unreachable over JMX
            read++;
            if (sc > 0) schemaTotal += sc;
            if (pl > 0) placementsTotal += pl;
        }
        boolean observed = (schemaTotal + placementsTotal) > 0;
        if (observed)
            harness.state.counters.put("coordinator_behind_meter_total",
                                       schemaTotal + placementsTotal);

        ObjectNode d = Harness.details();
        d.put("nodes_read", read);
        d.put("coordinator_behind_schema_total", schemaTotal);
        d.put("coordinator_behind_placements_total", placementsTotal);
        Assert.sometimes(observed, COORDINATOR_BEHIND, d);
    }

    // -------------------------------------------------------------------------------------------
    // Membership churn
    // -------------------------------------------------------------------------------------------

    // StorageService.operationMode() maps NodeState onto these (StorageService.java:3787-3813):
    //   REGISTERED               -> STARTING       a registered node not in the ring: a spare
    //   BOOTSTRAPPING/BOOT_REPLACING -> JOINING    a movement already in flight: leave it alone
    //   JOINED                   -> NORMAL         in the ring
    //   LEAVING                  -> LEAVING        in the ring, on its way out
    //   MOVING                   -> MOVING         in the ring, relocating
    //   LEFT                     -> DECOMMISSIONED out of the ring; needs unregister+wipe to return
    // Plus transient modes: JOINING_FAILED, DECOMMISSION_FAILED, MOVE_FAILED, DRAINING, DRAINED.

    /**
     * Nodes currently acting as replicas. Used for the never-drop-below-RF guard, so it counts every
     * node the placements still rely on -- including one that is LEAVING or MOVING, because its
     * ranges have not been handed over yet.
     */
    private List<Node> ringNodes()
    {
        List<Node> ring = new ArrayList<>();
        for (Node n : harness.nodes)
        {
            String mode = n.operationMode();
            if ("NORMAL".equals(mode) || "LEAVING".equals(mode) || "MOVING".equals(mode))
                ring.add(n);
        }
        return ring;
    }

    /**
     * Nodes in the ring and not already mid-operation -- the only valid targets for starting a new
     * decommission or move. A node that is LEAVING or MOVING already has a sequence in flight, and
     * starting a second one on it would be rejected for having an existing sequence rather than
     * testing anything.
     */
    private List<Node> idleRingNodes()
    {
        List<Node> idle = new ArrayList<>();
        for (Node n : harness.nodes)
        {
            if ("NORMAL".equals(n.operationMode()))
                idle.add(n);
        }
        return idle;
    }

    /**
     * Registered-but-not-joined nodes: the {@code join_ring=false} spares, which report STARTING
     * because their NodeState is REGISTERED.
     *
     * <p>Deliberately excludes JOINING: a node already bootstrapping has a sequence in flight, and
     * calling joinRing() on it again would be a no-op or an error rather than new coverage.
     */
    private List<Node> spareNodes()
    {
        List<Node> spares = new ArrayList<>();
        for (Node n : harness.nodes)
        {
            if ("STARTING".equals(n.operationMode()))
                spares.add(n);
        }
        return spares;
    }

    /** Seed hostnames (from WORKLOAD_SEEDS); replace-node leaves these alone to keep discovery stable. */
    private java.util.Set<String> seedHosts()
    {
        java.util.Set<String> seeds = new java.util.HashSet<>();
        for (String s : System.getenv().getOrDefault("WORKLOAD_SEEDS", "").split(","))
            if (!s.trim().isEmpty())
                seeds.add(s.trim());
        return seeds;
    }

    /**
     * Broadcast IPs of the current CMS members, parsed from {@code describeCMS().MEMBERS}
     * (format {@code "(nodeid=1,address=/10.0.0.1:7000),..."}). replace-node must never target a CMS
     * member: replace wipes the node's data including its copy of the metadata log, and a wiped CMS
     * member re-bootstrapping tries to fetch the CMS log from itself and dies (observed in the
     * 4-node smoke: candidates=[self], KeyspaceNotDefinedException, exit 3). Replacing a data node is
     * safe because it fetches the log from the live CMS members.
     */
    private java.util.Set<String> cmsMemberAddresses(Node node)
    {
        java.util.Set<String> addrs = new java.util.HashSet<>();
        Map<String, String> cms = node.describeCMS();
        if (cms == null)
            return addrs;
        java.util.regex.Matcher m =
            java.util.regex.Pattern.compile("address=/?([0-9A-Fa-f:.]+):\\d+")
                                   .matcher(cms.getOrDefault("MEMBERS", ""));
        while (m.find())
            addrs.add(m.group(1));
        return addrs;
    }

    /** State-ledger key marking a host that has been permanently replaced (do not restart it). */
    private static String replacedTag(String host)
    {
        return "replaced-host:" + host;
    }

    /**
     * A host that a replace consumed: it was stopped, its tokens were taken over by another node, and
     * its NodeId is LEFT in metadata. Restarting it would boot a stale, already-replaced identity, so
     * every recovery path must leave it down.
     */
    private boolean isReplaced(Node n)
    {
        return harness.state.tagLedger.containsKey(replacedTag(n.host));
    }

    private static boolean addressInList(List<String> ipPorts, String ip)
    {
        if (ip == null)
            return false;
        for (String e : ipPorts)
            if (e.equals(ip) || e.startsWith(ip + ":") || e.startsWith(ip + "/"))
                return true;
        return false;
    }

    private static void quietSleep(long millis)
    {
        try { Thread.sleep(millis); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /**
     * {@code r-node-replaced} / node-replacement coverage. Drives the real replace multi-step
     * operation (PREPARE/START/MID/FINISH_REPLACE) that no other driver exercises: a spare takes over
     * a killed ring node's tokens via a DIFFERENT-address replace. The reachability assertion ("a
     * node completed a replacement") lives SUT-side in {@link org.apache.cassandra.tcm.log.LocalLog}
     * keyed on FINISH_REPLACE enactment, so it is reliable regardless of workload polling; this
     * method makes it reachable and re-runs every range-movement safety invariant (locked ranges,
     * progress barriers, log integrity, serialization round-trip) over the replace path.
     *
     * <p>Why a spare and not replace-same-address: the 6-node smoke showed replace-same-address is
     * treated by TCM as a re-bootstrap (BootstrapAndJoin), NOT the replace MSO, so it would never
     * enact FINISH_REPLACE. A distinct replacement node is required to drive BootstrapAndReplace.
     *
     * <p>Flow: pick a victim (NORMAL, non-seed, and NOT a CMS member -- a wiped CMS member cannot
     * fetch the metadata log to rebuild) and a spare replacement. Stop the victim, wait for peers to
     * mark it down (a replace of a live node is rejected by design), then have the spare re-bootstrap
     * with {@code replace_address_first_boot=<victim>}. The victim is left down permanently and tagged
     * in the state ledger so no recovery path restarts its now-replaced identity.
     *
     * <p>Accounting/stability: each replace consumes one spare and retires one node, so the ring size
     * is unchanged and the operation is self-limiting to the number of spares (~3/run). It only runs
     * when a spare exists and more than RF nodes are NORMAL. A replace that stalls under injected
     * faults is left for check-recovery / e-cluster-converges-after-faults -- a real thing to test.
     */
    public void replaceNode()
    {
        // An available cold spare: node-agent reachable but Cassandra not yet started, so its address
        // has never been registered -- the one kind of node that can do a different-address replace.
        // Once consumed it is a running ring member and drops out of this list naturally.
        List<Node> spares = new ArrayList<>();
        for (Node c : harness.coldSpares)
            if (c.agentStatus() != null && !c.agentSaysRunning())
                spares.add(c);
        if (spares.isEmpty())
        {
            Log.info("replace-node: skipped (no cold spare available as replacement; coldSpares="
                     + harness.coldSpares + ")");
            return;
        }

        List<Node> idle = idleRingNodes();
        java.util.Set<String> seeds = seedHosts();

        // CMS members are read from any responsive ring node (all agree via ClusterMetadata).
        java.util.Set<String> cmsAddrs = java.util.Collections.emptySet();
        for (Node n : idle)
        {
            java.util.Set<String> a = cmsMemberAddresses(n);
            if (!a.isEmpty()) { cmsAddrs = a; break; }
        }

        List<Node> candidates = new ArrayList<>();
        for (Node n : idle)
            if (!seeds.contains(n.host) && !cmsAddrs.contains(n.broadcastAddress()) && !isReplaced(n))
                candidates.add(n);

        // Keep more than RF nodes NORMAL so retiring one for the replace cannot, by itself, drop live
        // replicas below a quorum.
        if (idle.size() <= Harness.PROBE_RF || candidates.isEmpty())
        {
            Log.info("replace-node: skipped (NORMAL=" + idle.size()
                     + ", eligible victims=" + candidates.size() + " [excl. seeds " + seeds
                     + " and CMS " + cmsAddrs + "], spares=" + spares.size()
                     + ", RF=" + Harness.PROBE_RF + ")");
            return;
        }

        Node victim = candidates.get(harness.random.nextInt(candidates.size()));
        Node replacement = spares.get(harness.random.nextInt(spares.size()));
        Node observer = null;
        for (Node n : idle)
            if (n != victim) { observer = n; break; }
        if (observer == null)
        {
            Log.info("replace-node: no live observer available");
            return;
        }

        String addr = victim.broadcastAddress();
        ObjectNode d = Harness.details();
        d.put("victim", victim.host);
        d.put("victim_address", addr);
        d.put("replacement", replacement.host);
        d.put("observer", observer.host);

        Log.info("replace-node: stopping victim " + victim.host + " (to be replaced by " + replacement.host + ")");
        victim.stopViaAgent(true);
        harness.state.bump("replace_attempts");

        // A replace is rejected while the cluster still sees the victim as live, so wait for the
        // observer to mark it unreachable (or give up and restart it, leaving the ring intact).
        boolean observedDown = false;
        long downDeadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < downDeadline)
        {
            if (!victim.agentSaysRunning() && addressInList(observer.unreachableNodes(), addr))
            {
                observedDown = true;
                break;
            }
            quietSleep(2_000);
        }
        d.put("observed_down", observedDown);

        if (!observedDown)
        {
            Log.info("replace-node: victim not seen down within budget; restarting it, no replace");
            victim.agent("POST", "/restart", "");
            Log.info("replace-node: " + d);
            return;
        }

        // Point of no return: the victim is now permanently replaced. Tag it BEFORE launching so that
        // even if this command dies mid-replace, no recovery path restarts its stale identity.
        harness.state.tagLedger.put(replacedTag(victim.host), addr);
        harness.state.save();

        Log.info("replace-node: " + replacement.host + " replacing " + victim.host + " (" + addr + ")");
        replacement.replaceWith(addr);
        harness.state.bump("replace_launched");

        // Wait for the replacement to reach NORMAL. If faults stall it, leave it for check-recovery;
        // do not block the driver indefinitely.
        boolean rejoined = false;
        long joinDeadline = System.currentTimeMillis() + 300_000;
        while (System.currentTimeMillis() < joinDeadline)
        {
            if ("NORMAL".equals(replacement.operationMode()))
            {
                rejoined = true;
                break;
            }
            quietSleep(5_000);
        }
        d.put("replacement_normal", rejoined);
        if (rejoined)
            harness.state.bump("replace_completed");
        Log.info("replace-node: " + d);
    }

    /**
     * {@code r-sequence-cancelled}. Abort a failed/stuck bootstrap and let TCM roll it back. Start a
     * spare joining, kill it mid-bootstrap (which both stalls the sequence and makes it abortable --
     * the SUT refuses to abort a live node), wait for peers to mark it down, then abortBootstrap,
     * which commits CancelInProgressSequence + Unregister. The reachability assertion (CANCEL_SEQUENCE
     * enacted) is SUT-side in {@link org.apache.cassandra.tcm.log.LocalLog}; the range-movement safety
     * invariants (b-locked-ranges-match-sequences, b-no-overlapping-locked-ranges) then evaluate the
     * rollback -- an abort that orphaned a lock or left a half-applied movement would trip them.
     *
     * <p>The spare is recycled with wipe-and-restart: the abort Unregistered it, so it re-registers
     * cleanly as a fresh spare (no ghost, no pool depletion). Ring size is unchanged throughout -- the
     * aborted join never joined -- so this is safe to run alongside the other churn drivers.
     */
    public void abortSequence()
    {
        List<Node> spares = spareNodes();
        if (spares.isEmpty())
        {
            Log.info("abort-sequence: skipped (no spare to start an abortable bootstrap)");
            return;
        }
        Node s = spares.get(harness.random.nextInt(spares.size()));
        Node observer = null;
        for (Node n : idleRingNodes()) { observer = n; break; }
        if (observer == null)
        {
            Log.info("abort-sequence: no live observer available");
            return;
        }

        String addr = s.broadcastAddress();
        ObjectNode d = Harness.details();
        d.put("spare", s.host);
        d.put("observer", observer.host);
        d.put("pre_normal", idleRingNodes().size());

        Log.info("abort-sequence: starting bootstrap on spare " + s.host);
        fireAndForget("abort-join-" + s.host, s::joinRing);
        harness.state.bump("abort_bootstrap_started");

        // Wait until the spare is actually mid-bootstrap before killing+aborting it. Poll tightly:
        // with no faults a bootstrap completes in well under a second (locally it often reaches NORMAL
        // before we can catch it, and the driver then cleanly skips), but under Antithesis fault
        // injection the bootstrap stalls for seconds-to-minutes, making the JOINING window easy to
        // catch. Treat either a non-STARTING operationMode OR the appearance of an in-progress
        // sequence as "mid-bootstrap".
        boolean joining = false;
        long joinDeadline = System.currentTimeMillis() + 90_000;
        while (System.currentTimeMillis() < joinDeadline)
        {
            String mode = s.operationMode();
            if ("NORMAL".equals(mode))
                break; // finished before we could catch it mid-flight
            if ((mode != null && !"STARTING".equals(mode)) || currentInProgressSequences() >= 1)
            {
                joining = true; // JOINING / BOOT_REPLACING, or a sequence is registered
                break;
            }
            quietSleep(250);
        }
        d.put("reached_joining", joining);
        if (!joining)
        {
            Log.info("abort-sequence: spare did not reach a mid-bootstrap state (mode="
                     + s.operationMode() + "); nothing to abort. " + d);
            return;
        }

        // Kill mid-bootstrap: stalls the sequence and makes the node abortable (SUT rejects aborting a
        // live node).
        Log.info("abort-sequence: killing " + s.host + " mid-bootstrap");
        s.stopViaAgent(true);
        boolean down = false;
        long downDeadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < downDeadline)
        {
            if (!s.agentSaysRunning() && addressInList(observer.unreachableNodes(), addr))
            {
                down = true;
                break;
            }
            quietSleep(2_000);
        }
        d.put("observed_down", down);
        if (!down)
        {
            Log.info("abort-sequence: spare not seen down in time; recycling without abort. " + d);
            s.agent("POST", "/wipe-and-restart?force=1", "{}");
            return;
        }

        Log.info("abort-sequence: abortBootstrap for " + s.host + " via " + observer.host);
        observer.abortBootstrap("", s.host);
        harness.state.bump("abort_bootstrap_committed");

        // Recycle: the abort Unregistered the spare, so wipe-and-restart re-registers it as a fresh
        // spare (join_ring=false base flag) back in the pool.
        s.agent("POST", "/wipe-and-restart?force=1", "{}");
        d.put("post_normal", idleRingNodes().size());
        Log.info("abort-sequence: " + d);
    }

    /**
     * One randomly chosen membership operation, fire-and-forget so it can overlap the next one.
     *
     * <p>Fire-and-forget is what makes {@code r-concurrent-multistep-operations} reachable: waiting
     * for each sequence to complete would serialise them and that property would never fire.
     */
    public void membershipChurn()
    {
        List<Node> ring = ringNodes();
        List<Node> idle = idleRingNodes();
        List<Node> spares = spareNodes();
        int choice = harness.random.nextInt(100);

        ObjectNode d = Harness.details();
        d.put("ring_count", ring.size());
        d.put("idle_ring_count", idle.size());
        d.put("spare_count", spares.size());

        if (choice < 30 && !spares.isEmpty())
        {
            Node target = spares.get(harness.random.nextInt(spares.size()));
            Log.info("joinRing on spare " + target.host);
            target.joinRing();
            harness.state.bump("joins_requested");
        }
        else if (choice < 60 && ring.size() > Harness.PROBE_RF && !idle.isEmpty())
        {
            // R9: only decommission while the ring stays at or above RF. Below that, TCM itself
            // rejects it, and forcing it would make the RF property fire on correct behaviour.
            // (This slice previously also issued `move`, but `move` is unsupported with vnodes --
            // num_tokens>1 -- so it always failed; removed. Concurrent movements are handled by the
            // dedicated concurrent-movements command via joins.)
            Node target = idle.get(harness.random.nextInt(idle.size()));
            Log.info("decommission " + target.host + " (ring size " + ring.size() + ")");
            target.decommission(false);
            harness.state.bump("decommissions_requested");
        }
        else if (choice < 75 && ring.size() > Harness.PROBE_RF && !ring.isEmpty())
        {
            // Any ring node, including one mid-sequence: restarting a node whose sequence is in
            // flight is exactly the b-sequence-resumable-after-crash window that
            // TCM_implementation.md invites ("the node may crash after executing PrepareJoin but
            // before it updates tokens in the local keyspace").
            Node target = ring.get(harness.random.nextInt(ring.size()));
            boolean force = harness.random.nextBoolean();
            Log.info("restart " + target.host + " (force=" + force + ")");
            target.agent("POST", "/restart" + (force ? "?force=1" : ""), "{}");
            harness.state.bump("restarts_requested");
        }
        else if (choice < 85 && ring.size() > Harness.PROBE_RF && !ring.isEmpty())
        {
            // Stop and leave stopped for a while: this is the window
            // b-sequence-resumable-after-crash cares about. The eventually_ command restarts
            // everything before asserting, which is what makes that property sound.
            Node target = ring.get(harness.random.nextInt(ring.size()));
            Log.info("stop " + target.host);
            target.agent("POST", "/stop?force=1", "{}");
            harness.state.bump("stops_requested");
        }
        else if (choice < 92 && !spares.isEmpty())
        {
            // wipe-and-rejoin: pushes a node back through Startup/Discovery *during* the
            // fault-injected phase. This is the only way a-metadata-identifier-unique and
            // c-initialization-abort-recoverable are reachable, because Antithesis injects no
            // faults before setup_complete and the cluster is already initialised by then.
            Node target = spares.get(harness.random.nextInt(spares.size()));
            unregisterLeftNodes();
            Log.info("wipe-and-restart " + target.host);
            target.agent("POST", "/wipe-and-restart?force=1", "{}");
            harness.state.bump("wipe_rejoins_requested");
        }
        else
        {
            // Bring anything that is stopped back up. Without this the ring would only ever shrink
            // and the workload would run out of things to do.
            int restarted = 0;
            for (Node n : harness.nodes)
            {
                if (isReplaced(n))
                    continue; // permanently replaced: leave it down
                if (!n.agentSaysRunning())
                {
                    Log.info("starting stopped node " + n.host);
                    n.agent("POST", "/start", "{}");
                    restarted++;
                }
            }
            d.put("restarted_stopped_nodes", restarted);
            harness.state.bump("recovery_starts");
        }

        d.put("choice", choice);
        Log.info("membership churn: " + d);
    }

    /**
     * {@code r-concurrent-multistep-operations} -- deliberately launch two or more range movements
     * that overlap in time. This is the DRIVER only; the assertion that owns the property message
     * ("two or more multi-step operations were in flight at once") lives SUT-side in
     * {@link org.apache.cassandra.tcm.log.LocalLog}, at the single point where every metadata
     * transition becomes visible.
     *
     * <p>Why the assertion moved out of the workload: run f863fad2...-59-13 launched two concurrent
     * joins but the workload's JMX polling only ever observed {@code peak_in_flight=1} -- the overlap
     * window can open and close between samples, so a poll-based {@code Sometimes} is unreliable.
     * Checking {@code inProgressSequences.size() >= 2} inside the log-enactment path catches the
     * transient regardless of poll cadence. The burst launched here is what drives the SUT into that
     * state (and makes the admission-safety properties {@code b-no-overlapping-locked-ranges} and
     * {@code b-locked-ranges-match-sequences} non-vacuous); the {@code peak_in_flight} sampling below
     * is retained as workload telemetry only.
     *
     * <p>Operation choice favours what is both concurrent and safe:
     * <ul>
     *   <li><b>joins</b> on available spares — each bootstraps into its own token range, additive so
     *       never below RF;
     *   <li><b>moves</b> on distinct NORMAL nodes to independent random tokens — keep the ring size
     *       constant (so always RF-safe) and are sustainable when no spares remain. Random tokens
     *       across the 2^64 space are, with overwhelming probability, far enough apart to be admitted
     *       concurrently rather than one being rejected for range overlap.
     * </ul>
     * Everything is fire-and-forget; waiting would serialise the very overlap we want.
     */
    public void launchConcurrentMovements()
    {
        List<Node> ring = ringNodes();
        List<Node> spares = spareNodes();
        List<Node> idle = idleRingNodes();

        List<String> launched = new ArrayList<>();
        java.util.Set<String> used = new java.util.HashSet<>();

        // CRITICAL: StorageServiceMBean.joinRing() and decommission() are `synchronized` / block
        // until the operation completes. Calling them sequentially on the workload thread would
        // SERIALISE them -- which is exactly why run 8bf9b2c6...-59-13 never saw concurrency (ex=0):
        // every operation ran to completion before the next began. Each is therefore launched on its
        // own thread so the blocking JMX calls run at the same time and the sequences overlap.
        //
        // Why joins (and decommission), not moves: this topology uses vnodes (num_tokens>1), and
        // `nodetool move` is unsupported there ("this node has more than one token and cannot be
        // moved thusly"). The available multi-step operations are bootstrap (join), leave
        // (decommission) and replace. Two concurrent joins now get *disjoint* random tokens because
        // the entrypoint disables allocate_tokens_for_local_replication_factor (which otherwise makes
        // both spares pick identical tokens, rejecting the second).

        // Preferred: two concurrent joins of spares -> two BootstrapAndJoin sequences at once.
        for (Node spare : spares)
        {
            if (launched.size() >= 2)
                break;
            final Node s = spare;
            Log.info("concurrent: joinRing on spare " + s.host);
            fireAndForget("join-" + s.host, s::joinRing);
            launched.add("join:" + s.host);
            used.add(s.host);
            harness.state.bump("concurrent_joins_requested");
        }

        // Fallback when fewer than two spares remain: pair a join with a decommission, or two
        // decommissions, keeping the ring at or above RF. TCM itself rejects a decommission that
        // would drop below RF, so only decommission while ring size exceeds RF (and exceeds RF+1 for
        // a second one). This is the natural join+leave concurrency the feature is designed for.
        int ringSize = ring.size();
        for (Node node : idle)
        {
            if (launched.size() >= 2)
                break;
            if (used.contains(node.host))
                continue;
            // Each decommission we launch will eventually remove one node; guard against dropping
            // below RF by accounting for how many we have already launched.
            int decommissionsLaunched = (int) launched.stream().filter(x -> x.startsWith("decommission:")).count();
            if (ringSize - decommissionsLaunched <= Harness.PROBE_RF)
                break; // no more headroom
            final Node nd = node;
            Log.info("concurrent: decommission " + nd.host + " (ring " + ringSize + ")");
            fireAndForget("decommission-" + nd.host, () -> nd.decommission(false));
            launched.add("decommission:" + nd.host);
            used.add(nd.host);
            harness.state.bump("concurrent_decommissions_requested");
        }

        // Tight sampling window: catch the overlap while the launched sequences are still in flight.
        // Poll as fast as dumpDirectory allows (no sleep). On a healthy no-fault cluster, progress
        // barriers pass instantly so sequences can complete in well under a second -- which is why a
        // 1s sampling interval saw only peak=1 locally even though both operations were admitted.
        // Under Antithesis, injected partitions stall the barriers and the sequences persist for
        // seconds to minutes, so the overlap is easy to catch; the tight poll just also gives a
        // fighting chance locally.
        int peak = 0;
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline)
        {
            peak = Math.max(peak, currentInProgressSequences());
            if (peak >= 2)
                break;
        }
        // Telemetry only -- the property assertion is SUT-side in LocalLog (see javadoc). We still
        // record what the workload could observe from outside so triage can compare the externally
        // visible peak against the SUT-side truth.
        if (peak > harness.state.counter("max_concurrent_sequences"))
            harness.state.counters.put("max_concurrent_sequences", (long) peak);
        Log.info("concurrent movements: launched=" + launched + " peak_in_flight=" + peak
                 + " (assertion is SUT-side in LocalLog)");
    }

    /**
     * Runs a blocking operation on a daemon thread so several can overlap. Daemon so a still-running
     * bootstrap/move never keeps the test-command JVM alive past its work; exceptions are logged,
     * not propagated (a movement failing under faults is expected, not a workload error).
     */
    private Thread fireAndForget(String name, Runnable op)
    {
        Thread t = new Thread(() -> {
            try
            {
                op.run();
            }
            catch (RuntimeException e)
            {
                Log.debug("concurrent op " + name + " ended: " + e);
            }
        }, "concurrent-" + name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Max in-progress multi-step operations across all reachable nodes right now.
     *
     * <p>Counts the number of *directory entries* (each keyed by a distinct owning node id) that
     * carry a {@code multi_step_operation}, NOT the number of distinct mso *strings*. Run
     * 32d96d63...-59-13 showed two joins genuinely in flight but reported peak=1, because both
     * bootstraps were at the same phase and rendered an identical mso map -- a
     * {@code Set<String>} of the rendered values collapsed them to one. Two sequences owned by two
     * different nodes are two sequences regardless of how similarly they render.
     */
    private int currentInProgressSequences()
    {
        int max = 0;
        for (Node n : harness.nodes)
        {
            Map<Long, Map<String, String>> dir = n.dumpDirectory(false);
            if (dir == null || dir.isEmpty())
                continue;
            int count = 0;
            for (Map<String, String> row : dir.values())
            {
                String mso = row.get("multi_step_operation");
                if (mso != null && !mso.isEmpty() && !"{}".equals(mso.trim()))
                    count++;
            }
            max = Math.max(max, count);
        }
        return max;
    }

    /**
     * Unregisters LEFT nodes. Required before a wipe-and-rejoin: a wiped node that is still
     * registered would try to rejoin holding a NodeId whose tokens are already assigned, which is a
     * legitimate rejection rather than the discovery path the property needs.
     */
    public void unregisterLeftNodes()
    {
        for (Node n : harness.nodes)
        {
            Map<Long, Map<String, String>> dir = n.dumpDirectory(false);
            if (dir == null || dir.isEmpty())
                continue;
            List<String> left = new ArrayList<>();
            for (Map.Entry<Long, Map<String, String>> e : dir.entrySet())
            {
                if ("LEFT".equalsIgnoreCase(String.valueOf(e.getValue().get("state"))))
                    left.add(String.valueOf(e.getKey()));
            }
            if (!left.isEmpty())
            {
                Log.info("unregistering LEFT nodes " + left + " via " + n.host);
                n.unregisterLeftNodes(left);
            }
            return; // one reachable node is enough; this is a cluster-wide operation
        }
    }

    // -------------------------------------------------------------------------------------------
    // CMS churn
    // -------------------------------------------------------------------------------------------

    /**
     * CMS-focused churn: reconfiguration, snapshots, cancel/resume, and commit pausing.
     *
     * <p>Reconfiguration RF alternates so membership actually moves repeatedly; setting it once
     * would produce at most one reconfiguration per timeline and
     * {@code r-cms-reconfiguration-observed} would rarely fire.
     */
    public void cmsChurn()
    {
        List<Node> live = new ArrayList<>();
        for (Node n : harness.nodes)
            if (n.describeCMS() != null)
                live.add(n);
        if (live.isEmpty())
        {
            Log.debug("no reachable node for CMS churn");
            return;
        }

        Node via = live.get(harness.random.nextInt(live.size()));
        int choice = harness.random.nextInt(100);

        if (choice < 35)
        {
            // R9: never request an RF the live node count cannot support -- that is a rejection,
            // not a reconfiguration, and it would satisfy neither this property nor the ones it
            // guards.
            int maxRf = Math.max(1, Math.min(3, live.size()));
            int rf = harness.random.nextBoolean() ? 1 : maxRf;
            if (rf > live.size())
            {
                Log.info("declining reconfigureCMS(" + rf + "): only " + live.size()
                         + " nodes reachable");
                harness.state.bump("reconfigure_declined");
            }
            else
            {
                Log.info("reconfigureCMS(" + rf + ") via " + via.host);
                via.reconfigureCMS(rf);
                harness.state.bump("reconfigure_requests");
            }
        }
        else if (choice < 55)
        {
            // Snapshots make r-snapshot-catchup-used reachable: a lagging node can only be served a
            // snapshot if one exists in the log.
            Log.info("snapshotClusterMetadata via " + via.host);
            via.snapshotClusterMetadata();
            harness.state.bump("snapshots_requested");
        }
        else if (choice < 70)
        {
            Log.info("resumeReconfigureCms via " + via.host);
            via.resumeReconfigureCms();
            harness.state.bump("reconfigure_resumes");
        }
        else if (choice < 80)
        {
            // Only cancel when something is actually in flight. Cancelling otherwise throws
            // "Can not cancel reconfiguration since there does not seem to be any in-flight", which
            // the loud mutator logging would report as a failure -- warning noise the workload
            // created itself, and the fastest way to train a reader to ignore real warnings.
            Map<String, List<String>> status = via.reconfigureCMSStatus();
            if (status != null && !status.isEmpty())
            {
                Log.info("cancelReconfigureCms via " + via.host + " (in flight: " + status + ")");
                via.cancelReconfigureCms();
                harness.state.bump("reconfigure_cancels");
            }
            else
            {
                Log.info("no CMS reconfiguration in flight; skipping cancel");
                harness.state.bump("reconfigure_cancel_skipped");
            }
        }
        else if (choice < 90)
        {
            // Cancel a sequence mid-flight. This is the operator action that
            // b-locked-ranges-match-sequences is about, and it is deliberately triggered rather
            // than waited for.
            Map<Long, Map<String, String>> dir = via.dumpDirectory(false);
            if (dir != null)
            {
                for (Map.Entry<Long, Map<String, String>> e : dir.entrySet())
                {
                    String mso = e.getValue().get("multi_step_operation");
                    if (mso != null && !mso.isEmpty() && !"{}".equals(mso.trim()))
                    {
                        // The node id is the *key* of the outer map, not a field in the row:
                        // ClusterMetadataDirectoryTable.directory() does
                        // result.put((long) nodeId.id(), row) and never puts node_id into the row.
                        String owner = String.valueOf(e.getKey());
                        Log.info("cancelInProgressSequences owner=" + owner);
                        via.cancelInProgressSequences(owner, null);
                        harness.state.bump("sequence_cancels");
                        break;
                    }
                }
            }
        }
        else
        {
            // Pause commits briefly, then clear. Always cleared here, and cleared again in the
            // eventually_ command, so e-cms-accepts-commits-after-recovery cannot fail because of
            // the workload's own action.
            Log.info("pausing commits on " + via.host);
            via.setCommitsPaused(true);
            harness.state.bump("commit_pauses");
            Checks.sleep(5_000);
            via.setCommitsPaused(false);
        }
    }

    /** Clears every workload-induced impediment before the quiet-period checks run. */
    public void clearWorkloadFaults()
    {
        for (Node n : harness.nodes)
        {
            n.setCommitsPaused(false);
            if (isReplaced(n))
                continue; // permanently replaced: restarting would boot a stale identity
            if (!n.agentSaysRunning())
            {
                Log.info("eventually: restarting " + n.host);
                n.agent("POST", "/start", "{}");
            }
        }
        // Give restarted nodes a chance to rejoin before anything is asserted. Faults have stopped
        // by the time an eventually_ command runs, but containers still need time to become
        // operational.
        Checks.sleep(30_000);

        // Nudge any stalled reconfiguration so a legitimately in-progress one is not misread as a
        // convergence failure.
        for (Node n : harness.nodes)
        {
            Map<String, List<String>> status = n.reconfigureCMSStatus();
            if (status != null && !status.isEmpty())
            {
                Log.info("resuming in-flight CMS reconfiguration via " + n.host);
                n.resumeReconfigureCms();
                break;
            }
        }
    }

    /** Blocks until every node answers CQL and JMX, or the deadline passes. */
    public boolean waitForCluster(long maxWaitMillis)
    {
        long deadline = System.currentTimeMillis() + maxWaitMillis;
        Map<String, String> lastState = new LinkedHashMap<>();
        while (System.currentTimeMillis() < deadline)
        {
            int ready = 0;
            lastState.clear();
            for (Node n : harness.nodes)
            {
                Map<String, String> cms = n.describeCMS();
                String mode = n.operationMode();
                lastState.put(n.host, mode + "/" + (cms == null ? "no-jmx"
                                                                : "epoch=" + cms.get("EPOCH")));
                if (cms != null && mode != null)
                    ready++;
            }
            if (ready == harness.nodes.size())
            {
                Log.info("all " + ready + " nodes ready: " + lastState);
                return true;
            }
            Log.info("waiting for cluster (" + ready + "/" + harness.nodes.size() + "): "
                     + lastState);
            Checks.sleep(5_000);
        }
        Log.warn("cluster not fully ready before deadline: " + lastState);
        return false;
    }
}
