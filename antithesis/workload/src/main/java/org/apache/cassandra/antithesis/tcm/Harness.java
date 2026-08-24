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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;

/**
 * Shared infrastructure for the TCM workload: node handles (JMX, pinned CQL session, control
 * agent), the load-balanced session, and the state that has to survive between test-command
 * invocations.
 *
 * <p>Antithesis runs each test command as a separate process. Anything the workload needs to
 * remember across commands -- per-node epoch high-water marks for
 * {@code a-epoch-monotonic-per-node}, the DDL tag ledger for
 * {@code c-commit-survives-cms-membership-change} -- therefore cannot live in memory. It is
 * persisted to {@link #STATE_FILE} and reloaded on each invocation.
 *
 * <p>Every remote call here can fail because Antithesis partitioned something. That is normal and
 * is never itself a property violation: callers get {@code null} or an empty result and are
 * expected to exclude that node from the current evaluation, recording the reduced node count so
 * {@code h-all-nodes-compared} can report when comparisons were incomplete.
 */
public final class Harness implements AutoCloseable
{
    public static final ObjectMapper JSON = new ObjectMapper();

    private static final String CMS_MBEAN = "org.apache.cassandra.tcm:type=CMSOperations";
    private static final String SS_MBEAN = "org.apache.cassandra.db:type=StorageService";

    static final Path STATE_DIR = Paths.get(System.getenv().getOrDefault(
        "WORKLOAD_STATE_DIR", "/var/lib/antithesis-workload"));
    static final Path STATE_FILE = STATE_DIR.resolve("state.json");

    /** Keyspace the workload owns. Probe reads/writes and schema churn happen here only. */
    public static final String PROBE_KEYSPACE = "antithesis_tcm";
    public static final String PROBE_TABLE = "probe";
    public static final int PROBE_RF = 3;

    public final List<Node> nodes;
    /**
     * Cold-spare hosts (WORKLOAD_COLD_SPARES): containers whose node-agent is up but whose Cassandra
     * is NOT started (NODE_AGENT_AUTOSTART=0), so their address is never registered until a replace
     * boots them. Kept OUT of {@link #nodes} so waitForCluster and the invariant checkers ignore them
     * -- they are only used by the replace-node driver as never-registered replacement nodes.
     */
    public final List<Node> coldSpares;
    private final Cluster balancedCluster;
    private Session balancedSession;
    public final Random random;
    public final State state;

    public Harness()
    {
        String hostList = System.getenv().getOrDefault(
            "CASSANDRA_NODES",
            "cassandra-1,cassandra-2,cassandra-3,cassandra-4,cassandra-5");
        List<Node> built = new ArrayList<>();
        for (String host : hostList.split(","))
        {
            String h = host.trim();
            if (!h.isEmpty())
                built.add(new Node(h));
        }
        this.nodes = Collections.unmodifiableList(built);

        List<Node> cold = new ArrayList<>();
        for (String host : System.getenv().getOrDefault("WORKLOAD_COLD_SPARES", "").split(","))
        {
            String h = host.trim();
            if (!h.isEmpty())
                cold.add(new Node(h));
        }
        this.coldSpares = Collections.unmodifiableList(cold);

        // Deterministic within a timeline: Antithesis controls the seed source, and a fixed seed
        // would make every timeline identical while System.nanoTime() would break replay. Reading
        // it from the environment lets Antithesis vary it while keeping a given timeline
        // reproducible.
        String seed = System.getenv("WORKLOAD_SEED");
        this.random = seed == null ? new Random() : new Random(Long.parseLong(seed));

        this.state = State.load();

        this.balancedCluster = Cluster.builder()
            .addContactPointsWithPorts(contactPoints())
            .withLoadBalancingPolicy(new RoundRobinPolicy())
            .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
            .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10_000)
                                                  .setReadTimeoutMillis(20_000))
            .withPoolingOptions(new PoolingOptions().setMaxConnectionsPerHost(HostDistance.LOCAL, 2))
            .withoutJMXReporting()
            .withoutMetrics()
            .build();
    }

    private List<InetSocketAddress> contactPoints()
    {
        List<InetSocketAddress> out = new ArrayList<>();
        for (Node n : nodes)
            out.add(new InetSocketAddress(n.host, 9042));
        return out;
    }

    /**
     * Load-balanced session, used for probe traffic and DDL. Lazily created so a command that only
     * needs JMX does not fail when CQL is unreachable.
     *
     * @return the session, or null if no node is currently reachable over CQL
     */
    public Session session()
    {
        if (balancedSession == null)
        {
            try
            {
                balancedSession = balancedCluster.connect();
            }
            catch (RuntimeException e)
            {
                Log.warn("no CQL session available: " + e);
                return null;
            }
        }
        return balancedSession;
    }

    public Node node(String host)
    {
        for (Node n : nodes)
            if (n.host.equals(host))
                return n;
        throw new IllegalArgumentException("unknown node " + host);
    }

    public Node randomNode()
    {
        return nodes.get(random.nextInt(nodes.size()));
    }

    @Override
    public void close()
    {
        for (Node n : nodes)
            n.close();
        try
        {
            balancedCluster.close();
        }
        catch (RuntimeException ignored)
        {
        }
        state.save();
    }

    /** One Cassandra node: JMX, a session pinned to it, and its control agent. */
    public final class Node implements AutoCloseable
    {
        public final String host;
        private JMXConnector connector;
        private MBeanServerConnection mbeans;
        private Cluster pinnedCluster;
        private Session pinnedSession;

        Node(String host)
        {
            this.host = host;
        }

        // ---- JMX ------------------------------------------------------------------------------

        private MBeanServerConnection mbeans() throws IOException
        {
            if (mbeans == null)
            {
                JMXServiceURL url = new JMXServiceURL(
                    "service:jmx:rmi:///jndi/rmi://" + host + ":7199/jmxrmi");
                connector = JMXConnectorFactory.connect(url, null);
                mbeans = connector.getMBeanServerConnection();
            }
            return mbeans;
        }

        private void dropJmx()
        {
            if (connector != null)
            {
                try
                {
                    connector.close();
                }
                catch (IOException ignored)
                {
                }
            }
            connector = null;
            mbeans = null;
        }

        /**
         * Invoke an MBean operation, returning null on any failure. A partitioned or stopped node
         * is an expected outcome, not an error -- so the connection is dropped and will be rebuilt
         * on the next attempt rather than being cached in a broken state.
         */
        @SuppressWarnings("unchecked")
        public <T> T invoke(String mbean, String operation, Object[] args, String[] signature)
        {
            try
            {
                return (T) mbeans().invoke(new ObjectName(mbean), operation, args, signature);
            }
            catch (Exception e)
            {
                Log.debug(host + ": invoke " + operation + " failed: " + e);
                dropJmx();
                return null;
            }
        }

        /**
         * Like {@link #invoke} but logs failures at WARN rather than DEBUG. Used for every
         * *mutating* call.
         *
         * <p>This distinction was learned the hard way: {@code reconfigureCMS(3)} was being rejected
         * server-side ("There are not enough nodes in datacenter1 datacenter to satisfy replication
         * factor") and, because the failure was logged at DEBUG and debug output is off by default,
         * the workload silently believed it had requested a reconfiguration that never happened.
         * A read that fails is expected under partition; a mutation that fails means an action the
         * workload thinks it performed did not occur, which invalidates whatever the checks then
         * conclude.
         */
        @SuppressWarnings("unchecked")
        public <T> T mutate(String mbean, String operation, Object[] args, String[] signature)
        {
            try
            {
                return (T) mbeans().invoke(new ObjectName(mbean), operation, args, signature);
            }
            catch (Exception e)
            {
                Log.warn(host + ": mutating call " + operation + " FAILED: " + rootCause(e));
                dropJmx();
                return null;
            }
        }

        @SuppressWarnings("unchecked")
        public <T> T attribute(String mbean, String name)
        {
            try
            {
                return (T) mbeans().getAttribute(new ObjectName(mbean), name);
            }
            catch (Exception e)
            {
                Log.debug(host + ": attribute " + name + " failed: " + e);
                dropJmx();
                return null;
            }
        }

        public void setAttribute(String mbean, String name, Object value)
        {
            try
            {
                mbeans().setAttribute(new ObjectName(mbean),
                                      new javax.management.Attribute(name, value));
            }
            catch (Exception e)
            {
                Log.debug(host + ": setAttribute " + name + " failed: " + e);
                dropJmx();
            }
        }

        /**
         * Reads the {@code Count} of a Dropwizard {@code Meter} exposed over JMX (e.g.
         * {@code org.apache.cassandra.metrics:type=TCM,name=CoordinatorBehindSchema}).
         *
         * @return the cumulative count, or -1 if the node/metric is unreachable. -1 lets callers
         *         distinguish "no data" from a real zero.
         */
        public long meterCount(String objectName)
        {
            Object v = attribute(objectName, "Count");
            if (v instanceof Number)
                return ((Number) v).longValue();
            return -1L;
        }

        // ---- CMSOperationsMBean ---------------------------------------------------------------

        /** {@code describeCMS()}; null if unreachable. Keys: MEMBERS, EPOCH, CMS_ID, ... */
        public Map<String, String> describeCMS()
        {
            return invoke(CMS_MBEAN, "describeCMS", new Object[0], new String[0]);
        }

        /** Local view of the metadata log, epoch -> {KIND, TRANSFORMATION, ENTRY_ID, ...}. */
        public Map<Long, Map<String, String>> dumpLog(long from, long to)
        {
            return invoke(CMS_MBEAN, "dumpLog",
                          new Object[]{ from, to },
                          new String[]{ "long", "long" });
        }

        /** Local view of the directory, nodeId -> field map. */
        public Map<Long, Map<String, String>> dumpDirectory(boolean includeTokens)
        {
            return invoke(CMS_MBEAN, "dumpDirectory",
                          new Object[]{ includeTokens },
                          new String[]{ "boolean" });
        }

        public Map<String, List<String>> reconfigureCMSStatus()
        {
            return invoke(CMS_MBEAN, "reconfigureCMSStatus", new Object[0], new String[0]);
        }

        public void reconfigureCMS(int rf)
        {
            mutate(CMS_MBEAN, "reconfigureCMS", new Object[]{ rf }, new String[]{ "int" });
        }

        public void cancelReconfigureCms()
        {
            mutate(CMS_MBEAN, "cancelReconfigureCms", new Object[0], new String[0]);
        }

        public void resumeReconfigureCms()
        {
            mutate(CMS_MBEAN, "resumeReconfigureCms", new Object[0], new String[0]);
        }

        public void snapshotClusterMetadata()
        {
            mutate(CMS_MBEAN, "snapshotClusterMetadata", new Object[0], new String[0]);
        }

        /**
         * Abort a stuck/failed bootstrap (or replace) for the node at {@code endpoint}, committing a
         * CancelInProgressSequence + Unregister. The target must be down first -- the SUT rejects
         * aborting a live node. Pass an empty nodeId and a hostname/endpoint; the SUT resolves it.
         */
        public void abortBootstrap(String nodeId, String endpoint)
        {
            mutate(SS_MBEAN, "abortBootstrap", new Object[]{ nodeId, endpoint },
                   new String[]{ "java.lang.String", "java.lang.String" });
        }

        public Boolean cancelInProgressSequences(String owner, String expectedKind)
        {
            return mutate(CMS_MBEAN, "cancelInProgressSequences",
                          new Object[]{ owner, expectedKind },
                          new String[]{ "java.lang.String", "java.lang.String" });
        }

        public void unregisterLeftNodes(List<String> nodeIds)
        {
            mutate(CMS_MBEAN, "unregisterLeftNodes",
                   new Object[]{ nodeIds }, new String[]{ "java.util.List" });
        }

        public void setCommitsPaused(boolean paused)
        {
            setAttribute(CMS_MBEAN, "CommitsPaused", paused);
        }

        // ---- StorageServiceMBean --------------------------------------------------------------

        public String schemaVersion()
        {
            return attribute(SS_MBEAN, "SchemaVersion");
        }

        public String operationMode()
        {
            return attribute(SS_MBEAN, "OperationMode");
        }

        public void joinRing()
        {
            mutate(SS_MBEAN, "joinRing", new Object[0], new String[0]);
        }

        public void decommission(boolean force)
        {
            mutate(SS_MBEAN, "decommission", new Object[]{ force }, new String[]{ "boolean" });
        }

        public void move(String token)
        {
            mutate(SS_MBEAN, "move", new Object[]{ token },
                   new String[]{ "java.lang.String" });
        }

        /**
         * Cassandra's own computed replica sets: range -> replicas. Used instead of reconstructing
         * placements from tokens, which would mean reimplementing the placement algorithm in the
         * workload and then testing the reimplementation (evaluation refinement R1).
         */
        public Map<List<String>, List<String>> rangeToEndpointMap(String keyspace)
        {
            return invoke(SS_MBEAN, "getRangeToEndpointWithPortMap",
                          new Object[]{ keyspace }, new String[]{ "java.lang.String" });
        }

        public Map<List<String>, List<String>> pendingRangeToEndpointMap(String keyspace)
        {
            return invoke(SS_MBEAN, "getPendingRangeToEndpointWithPortMap",
                          new Object[]{ keyspace }, new String[]{ "java.lang.String" });
        }

        public Map<String, String> tokenToEndpointMap()
        {
            return attribute(SS_MBEAN, "TokenToEndpointWithPortMap");
        }

        // ---- pinned CQL -----------------------------------------------------------------------

        /**
         * A session that will only ever coordinate through this node. Required by
         * {@code r-coordinator-behind-rejection} and {@code d-prepared-statement-not-stale}: a
         * default load-balancing policy routes around a lagging node, which is precisely the node
         * those properties need to query through.
         *
         * @return the pinned session, or null if this node is not reachable over CQL
         */
        public Session pinnedSession()
        {
            if (pinnedSession == null)
            {
                try
                {
                    InetSocketAddress addr = new InetSocketAddress(host, 9042);
                    pinnedCluster = Cluster.builder()
                        .addContactPointsWithPorts(addr)
                        .withLoadBalancingPolicy(
                            new WhiteListPolicy(new RoundRobinPolicy(),
                                                Collections.singletonList(addr)))
                        .withQueryOptions(new QueryOptions()
                            .setConsistencyLevel(ConsistencyLevel.QUORUM))
                        .withSocketOptions(new SocketOptions().setConnectTimeoutMillis(10_000)
                                                              .setReadTimeoutMillis(20_000))
                        .withoutJMXReporting()
                        .withoutMetrics()
                        .build();
                    pinnedSession = pinnedCluster.connect();
                }
                catch (RuntimeException e)
                {
                    Log.debug(host + ": no pinned CQL session: " + e);
                    closePinned();
                    return null;
                }
            }
            return pinnedSession;
        }

        private void closePinned()
        {
            if (pinnedCluster != null)
            {
                try
                {
                    pinnedCluster.close();
                }
                catch (RuntimeException ignored)
                {
                }
            }
            pinnedCluster = null;
            pinnedSession = null;
        }

        // ---- control agent --------------------------------------------------------------------

        /**
         * Call the node control agent. See docker/node-agent.py for why it exists: Antithesis node
         * termination faults are off by default, and node replacement needs a JVM flag that only a
         * process restart can apply.
         *
         * @return the agent's JSON response, or null if the agent could not be reached
         */
        public ObjectNode agent(String method, String path, String body)
        {
            HttpURLConnection conn = null;
            try
            {
                URL url = new URL("http://" + host + ":7788" + path);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod(method);
                conn.setConnectTimeout(5_000);
                conn.setReadTimeout(180_000); // a graceful stop can take a while
                if (body != null)
                {
                    conn.setDoOutput(true);
                    conn.setRequestProperty("Content-Type", "application/json");
                    byte[] payload = body.getBytes(StandardCharsets.UTF_8);
                    conn.setFixedLengthStreamingMode(payload.length);
                    try (OutputStream out = conn.getOutputStream())
                    {
                        out.write(payload);
                    }
                }
                try (java.io.InputStream in = conn.getInputStream())
                {
                    byte[] raw = readAll(in);
                    return (ObjectNode) JSON.readTree(raw);
                }
            }
            catch (Exception e)
            {
                Log.debug(host + ": agent " + method + " " + path + " failed: " + e);
                return null;
            }
            finally
            {
                if (conn != null)
                    conn.disconnect();
            }
        }

        public ObjectNode agentStatus()
        {
            return agent("GET", "/status", null);
        }

        public boolean agentSaysRunning()
        {
            ObjectNode status = agentStatus();
            return status != null && status.path("running").asBoolean(false);
        }

        /** Stop this node's Cassandra process via the control agent (graceful unless force). */
        public ObjectNode stopViaAgent(boolean force)
        {
            return agent("POST", "/stop" + (force ? "?force=1" : ""), "");
        }

        /**
         * Wipe this node's state and re-bootstrap it as the replacement for the (dead) node at
         * {@code victimAddress}, taking over that node's tokens via
         * {@code -Dcassandra.replace_address_first_boot=<victimAddress>}. When this node is a spare
         * (join_ring=false), the agent drops that flag so the replacement joins the ring. The caller
         * must have stopped the victim and waited for peers to mark it down first; a replace of a
         * node the cluster still sees as live is rejected by design. This drives the real
         * BootstrapAndReplace multi-step operation (PREPARE/START/MID/FINISH_REPLACE).
         */
        public ObjectNode replaceWith(String victimAddress)
        {
            return agent("POST", "/replace?address=" + victimAddress, "");
        }

        /**
         * Replace-same-address: wipe and re-bootstrap taking over this node's own previous position.
         * Note: TCM treats this as a re-bootstrap (BootstrapAndJoin), not the replace MSO -- use
         * {@link #replaceWith} against a distinct victim to exercise BootstrapAndReplace.
         */
        public ObjectNode replaceSelf()
        {
            return agent("POST", "/replace", "");
        }

        /** This node's broadcast address as peers see it (container hostname resolved to an IP). */
        public String broadcastAddress()
        {
            try
            {
                return java.net.InetAddress.getByName(host).getHostAddress();
            }
            catch (Exception e)
            {
                Log.debug(host + ": could not resolve broadcast address: " + e);
                return null;
            }
        }

        /** Endpoints (as {@code ip:port}) this node currently considers unreachable. */
        @SuppressWarnings("unchecked")
        public List<String> unreachableNodes()
        {
            List<String> v = attribute(SS_MBEAN, "UnreachableNodesWithPort");
            return v == null ? java.util.Collections.emptyList() : v;
        }

        /** Endpoints (as {@code ip:port}) this node currently considers live. */
        @SuppressWarnings("unchecked")
        public List<String> liveNodes()
        {
            List<String> v = attribute(SS_MBEAN, "LiveNodesWithPort");
            return v == null ? java.util.Collections.emptyList() : v;
        }

        @Override
        public void close()
        {
            dropJmx();
            closePinned();
        }

        @Override
        public String toString()
        {
            return host;
        }
    }

    /** Unwraps a JMX/RMI exception chain: the outer message is usually a generic wrapper. */
    static String rootCause(Throwable t)
    {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause)
            cause = cause.getCause();
        return cause == t ? String.valueOf(t) : t + " <- " + cause;
    }

    private static byte[] readAll(java.io.InputStream in) throws IOException
    {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int read;
        while ((read = in.read(chunk)) != -1)
            buf.write(chunk, 0, read);
        return buf.toByteArray();
    }

    /**
     * Workload state that must outlive a single test-command process.
     *
     * <p>Persisted as JSON. A corrupt or missing file yields empty state rather than an error: a
     * fresh timeline legitimately has none, and refusing to start would turn a first run into a
     * failure.
     */
    public static final class State
    {
        /** host -> highest epoch ever observed from that host, for a-epoch-monotonic-per-node. */
        public final Map<String, Long> highestEpochSeen = new LinkedHashMap<>();
        /** DDL tag -> outcome ("acked" | "unknown" | "rejected"), for c-commit-survives-*. */
        public final Map<String, String> tagLedger = new LinkedHashMap<>();
        /** Counters reported at the end of a run; see R5 and R10 in evaluation/synthesis.md. */
        public final Map<String, Long> counters = new LinkedHashMap<>();

        public static State load()
        {
            State s = new State();
            try
            {
                if (Files.exists(STATE_FILE))
                {
                    ObjectNode root = (ObjectNode) JSON.readTree(Files.readAllBytes(STATE_FILE));
                    root.path("highestEpochSeen").fields().forEachRemaining(
                        e -> s.highestEpochSeen.put(e.getKey(), e.getValue().asLong()));
                    root.path("tagLedger").fields().forEachRemaining(
                        e -> s.tagLedger.put(e.getKey(), e.getValue().asText()));
                    root.path("counters").fields().forEachRemaining(
                        e -> s.counters.put(e.getKey(), e.getValue().asLong()));
                }
            }
            catch (Exception e)
            {
                Log.warn("could not read workload state, starting empty: " + e);
            }
            return s;
        }

        public void bump(String counter)
        {
            counters.merge(counter, 1L, Long::sum);
        }

        public long counter(String name)
        {
            return counters.getOrDefault(name, 0L);
        }

        public void save()
        {
            // Per-process temp file. Antithesis runs each test command as its own process and they
            // share WORKLOAD_STATE_DIR, so a single fixed "state.json.tmp" is a race: process A
            // renames tmp->state.json (tmp now gone), process B's rename then fails with
            // NoSuchFileException. That surfaced in run 8bf9b2c6...-59-13 as ~10 non-zero exits of
            // anytime_check_tcm_invariants (the throw happened at close(), after assertions were
            // emitted, so it only tainted the exit code -- but a red command is still noise). A
            // unique temp name per process removes the collision; the final ATOMIC_MOVE to
            // state.json is itself atomic, so concurrent writers just resolve last-writer-wins.
            Path tmp = STATE_DIR.resolve("state.json." + ProcessHandle.current().pid()
                                         + "." + System.nanoTime() + ".tmp");
            try
            {
                Files.createDirectories(STATE_DIR);
                ObjectNode root = JSON.createObjectNode();
                ObjectNode epochs = root.putObject("highestEpochSeen");
                highestEpochSeen.forEach(epochs::put);
                ObjectNode tags = root.putObject("tagLedger");
                tagLedger.forEach(tags::put);
                ObjectNode counts = root.putObject("counters");
                counters.forEach(counts::put);
                Files.write(tmp, JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(root));
                Files.move(tmp, STATE_FILE,
                           java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                           java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            }
            catch (IOException | RuntimeException e)
            {
                // Best-effort: this file is diagnostic bookkeeping (epoch high-water marks, the tag
                // ledger). A failed save must not fail the test command or abort close() -- doing so
                // would report a workload problem as a red property. Log and move on. Worst case a
                // few observations are not carried to the next invocation.
                Log.warn("could not persist workload state (continuing): " + e);
                try
                {
                    Files.deleteIfExists(tmp);
                }
                catch (IOException ignored)
                {
                }
            }
        }
    }

    /** Details payload builder. Every assertion carries one so triage reports are actionable. */
    public static ObjectNode details()
    {
        return JSON.createObjectNode();
    }

    /**
     * Minimal logging: stdout only, no ANSI, since Antithesis stores raw bytes.
     *
     * <p>Debug is off unless {@code WORKLOAD_DEBUG=1}. Under fault injection every unreachable node
     * produces a debug line per JMX call per poll, and a multi-line RMI exception with it -- enough
     * to bury the workload's own findings in the triage report. Unreachable nodes are the expected
     * case here, not an anomaly worth logging.
     */
    public static final class Log
    {
        private static final boolean DEBUG =
            "1".equals(System.getenv().getOrDefault("WORKLOAD_DEBUG", "0"));

        public static void info(String msg)
        {
            System.out.println("[workload] " + msg);
        }

        public static void warn(String msg)
        {
            System.out.println("[workload][warn] " + msg);
        }

        public static void debug(String msg)
        {
            if (DEBUG)
                System.out.println("[workload][debug] " + msg);
        }
    }
}
