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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Minimal local stand-in for the Netflix Cassandra token service + swappie, for the local
 * multi-node cluster (./gradlew cassRun — see RUNNING_LOCALLY.md). It speaks plain HTTP (no
 * Metatron mTLS) and exercises the real in-container code paths:
 *
 *   - TokenService.getCurrentInstance()   -> GET  /v1/token/current
 *   - TokenServiceSeedProvider.getSeeds() -> GET  /v1/cluster/{env}/{app}
 *   - docker-entrypoint.sh poke_swappie   -> POST /api/swap/{env}/{app}/poke
 *
 * Each cluster member is declared with one --node flag and is served on its OWN port. A node
 * fetches its token from /v1/token/current on its own port, so each container is handed the
 * right token without relying on source-IP (Docker NATs container traffic). /v1/cluster returns
 * the whole cluster on every port, so the seed provider sees all members.
 *
 *   java local/LocalTokenServer.java --app cass_local --env local --region us-east-1 \
 *        --node id=node1,port=7011,ip=172.20.0.11,token=-9223372036854775808,az=us-east-1a \
 *        --node id=node2,port=7012,ip=172.20.0.12,token=-3074457345618258603,az=us-east-1b \
 *        --node id=node3,port=7013,ip=172.20.0.13,token=3074457345618258602,az=us-east-1c
 *
 * Dependency-free (JDK built-in HttpServer); runs as a single-file source program.
 */
public class LocalTokenServer
{
    // Global identity shared by all nodes.
    private static String app    = "cass_local";
    private static String env    = "local";
    private static String region = "us-east-1";
    private static boolean startAssigned = true;

    private static final List<Node> nodes = new ArrayList<>();

    static final class Node
    {
        final String id, ip, token, az, hostName;
        final int port;
        final AtomicBoolean assigned;

        Node(String id, int port, String ip, String token, String az, String hostName, boolean assigned)
        {
            this.id = id;
            this.port = port;
            this.ip = ip;
            this.token = token;
            this.az = az;
            this.hostName = hostName;
            this.assigned = new AtomicBoolean(assigned);
        }
    }

    public static void main(String[] args) throws Exception
    {
        parseArgs(args);

        if (nodes.isEmpty())
        {
            System.err.println("No --node given. Declare at least one node, e.g.\n"
                + "  --node id=node1,port=7011,ip=172.20.0.11,token=-9223372036854775808,az=us-east-1a");
            System.exit(2);
        }

        for (Node node : nodes)
        {
            HttpServer server = HttpServer.create(new InetSocketAddress(node.port), 0);
            server.createContext("/v1/token/current", ex -> handleTokenCurrent(ex, node));
            server.createContext("/v1/cluster", LocalTokenServer::handleCluster);
            server.createContext("/api/swap", ex -> handleSwappiePoke(ex, node));
            server.createContext("/", LocalTokenServer::handleRoot);
            server.setExecutor(null);
            server.start();
            log("node " + node.id + " on http://0.0.0.0:" + node.port
                + "  ip=" + node.ip + " token=" + node.token + " az=" + node.az
                + "  (" + (node.assigned.get() ? "ASSIGNED" : "UNASSIGNED, first /poke assigns") + ")");
        }
        log("serving " + nodes.size() + " node(s); app=" + app + " env=" + env + " region=" + region);

        // Keep the process (often container PID 1) alive.
        Thread.currentThread().join();
    }

    private static void handleTokenCurrent(HttpExchange ex, Node node) throws IOException
    {
        if (!"GET".equals(ex.getRequestMethod())) { respond(ex, 405, "method not allowed"); return; }
        if (node.assigned.get())
        {
            log("GET :" + node.port + "/v1/token/current -> 200 (" + node.id + " token=" + node.token + ")");
            respondJson(ex, 200, instanceJson(node));
        }
        else
        {
            log("GET :" + node.port + "/v1/token/current -> 404 (" + node.id + " unassigned)");
            respondJson(ex, 404, "{\"message\":\"no token assigned\"}");
        }
    }

    private static void handleCluster(HttpExchange ex) throws IOException
    {
        if (!"GET".equals(ex.getRequestMethod())) { respond(ex, 405, "method not allowed"); return; }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < nodes.size(); i++)
        {
            if (i > 0) sb.append(',');
            sb.append(instanceJson(nodes.get(i)));
        }
        sb.append(']');
        log("GET " + ex.getRequestURI().getPath() + " -> 200 (" + nodes.size() + " instances)");
        respondJson(ex, 200, sb.toString());
    }

    private static void handleSwappiePoke(HttpExchange ex, Node node) throws IOException
    {
        if (!"POST".equals(ex.getRequestMethod())) { respond(ex, 405, "method not allowed"); return; }
        boolean was = node.assigned.getAndSet(true);
        log("POST " + ex.getRequestURI().getPath() + " -> " + (was ? "200 (already assigned)"
                                                                    : "swappie assigned a token to " + node.id));
        respondJson(ex, 200, "{\"status\":\"ok\"}");
    }

    private static void handleRoot(HttpExchange ex) throws IOException
    {
        respond(ex, 200, "local token+swappie server\n"
                         + "GET  /v1/token/current\n"
                         + "GET  /v1/cluster/{env}/{app}\n"
                         + "POST /api/swap/{env}/{app}/poke\n");
    }

    // Field order/names/types mirror com.netflix.cassandra.NetflixInstance exactly; the in-JVM
    // Jackson ObjectMapper fails on unknown properties, so only emit fields that class declares.
    private static String instanceJson(Node n)
    {
        return "{"
             + "\"updateTime\":0,"
             + "\"createdTime\":0,"
             + "\"app\":\"" + app + "\","
             + "\"instanceId\":\"" + n.id + "\","
             + "\"availabilityZone\":\"" + n.az + "\","
             + "\"token\":\"" + n.token + "\","
             + "\"region\":\"" + region + "\","
             + "\"id\":1,"
             + "\"hostIP\":\"" + n.ip + "\","
             + "\"hostName\":\"" + n.hostName + "\","
             + "\"key\":\"" + app + "-" + n.id + "\""
             + "}";
    }

    private static void parseArgs(String[] args)
    {
        for (int i = 0; i < args.length; i++)
        {
            switch (args[i])
            {
                case "--app":              app = args[++i]; break;
                case "--env":              env = args[++i]; break;
                case "--region":           region = args[++i]; break;
                case "--start-unassigned": startAssigned = false; break;
                case "--start-assigned":   startAssigned = true; break;
                case "--node":             nodes.add(parseNode(args[++i])); break;
                case "-h":
                case "--help":
                    System.out.println("Usage: java local/LocalTokenServer.java [--app A] [--env E] [--region R]\n"
                        + "  --node id=node1,port=7011,ip=IP,token=T,az=AZ   (repeatable, one per cluster member)\n"
                        + "  [--start-unassigned]   start with no token until first swappie /poke");
                    System.exit(0);
                    break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    System.exit(2);
            }
        }
    }

    // Parse a --node spec like "id=node1,port=7011,ip=172.20.0.11,token=-92...,az=us-east-1a".
    private static Node parseNode(String spec)
    {
        Map<String, String> kv = new LinkedHashMap<>();
        for (String pair : spec.split(","))
        {
            int eq = pair.indexOf('=');
            if (eq < 0) { System.err.println("Bad --node field (expected k=v): " + pair); System.exit(2); }
            kv.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        String id = kv.getOrDefault("id", "node");
        if (!kv.containsKey("port") || !kv.containsKey("ip") || !kv.containsKey("token"))
        {
            System.err.println("--node requires at least port, ip, token: " + spec);
            System.exit(2);
        }
        return new Node(id, Integer.parseInt(kv.get("port")), kv.get("ip"), kv.get("token"),
                        kv.getOrDefault("az", "local-1a"), kv.getOrDefault("host", id), startAssigned);
    }

    private static void respondJson(HttpExchange ex, int code, String body) throws IOException
    {
        ex.getResponseHeaders().set("Content-Type", "application/json");
        respond(ex, code, body);
    }

    private static void respond(HttpExchange ex, int code, String body) throws IOException
    {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static void log(String msg)
    {
        System.out.println("[local-token-server] " + msg);
    }
}
