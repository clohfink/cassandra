# Running a local Cassandra cluster

This repo builds Cassandra as a container that, in production, runs in Netflix's **DGW**
environment: the platform injects the `cassandra.yaml`, assigns tokens through the **token
service**, nudges **swappie** for token/slot assignment, and authenticates everything with
**Metatron** mTLS.

None of that infrastructure exists on a laptop. `./gradlew cassRun` builds the same image and
brings up a local **3-node cluster**, with a tiny **local token + swappie server** standing in
for the managed services so the real in-container code paths (token assignment, seed discovery
via `TokenServiceSeedProvider`, the swappie poke loop) actually execute.

Production (DGW) behavior is unchanged — local behavior is gated behind a **`LOCAL_MODE`** that
the entrypoint auto-detects when `/etc/nflx/environment` is absent (or `CASSANDRA_LOCAL_MODE=true`).

---

## Quickstart

```bash
./gradlew cassRun
```

This single command:

1. builds Cassandra and the container image (`jibDockerBuild`);
2. renders `local/cass-template.yaml` into a home directory per node —
   `local/node{1,2,3}/{conf,data,commitlog,saved_caches}` (data lives on your host);
3. generates `local/cluster-compose.yml`; and
4. starts a **3-node cluster** plus a bundled token-service container.

The nodes mimic app **`cass_local`** in region **`us-east-1`** across racks **`1a`/`1b`/`1c`**.
The token service hands each node its own balanced Murmur3 token, and `TokenServiceSeedProvider`
discovers all three as seeds. node1 starts first; node2/node3 wait for it to be healthy, for
clean ring formation.

> **Apple Silicon:** the image is linux/amd64, so the nodes run under emulation and the cluster
> takes a few minutes to converge. See [Known limitations](#known-limitations).

Check it / use it:

```bash
# ring — expect 3 UN nodes, Datacenter: us-east-1, racks 1a/1b/1c
./bin/nodetool -h 127.0.0.1 -p 7501 status      # node1 (node2: -p 7502, node3: -p 7503)

# cqlsh to a node (native ports: node1 7104, node2 7105, node3 7106)
./bin/cqlsh 127.0.0.1 7104

# follow a node's logs
docker compose -f local/cluster-compose.yml logs -f node2
```

Stop / wipe:

```bash
./gradlew cassStop     # stop containers, keep per-node data on disk
./gradlew cassClean    # stop and wipe data/commitlog/saved_caches
```

---

## Prerequisites

- **Docker** (Docker Desktop on macOS/Windows, or Docker Engine on Linux).
- **Corp network access** for `jibDockerBuild` to pull the base image
  `dockerregistry.test.netflix.net:7002/cde/dgw-base-java21-jib:latest`. The build runs on your
  machine — only the base image is pulled. (The token-service container pulls `eclipse-temurin:21-jdk`.)

---

## What `cassRun` creates

```
local/
  cass-template.yaml        # the one config template, @NODE_LISTEN_ADDRESS@ rendered per node
  LocalTokenServer.java     # the stand-in token + swappie server (run as a container)
  cluster-compose.yml       # GENERATED — 3 cass nodes + tokenserver  (git-ignored)
  node1/  node2/  node3/    # GENERATED per-node homes               (git-ignored)
    conf/                   #   rendered cassandra.yaml + cassandra-rackdc.properties
    data/  commitlog/  saved_caches/   #   live data on your host
```

| Service | Image | Network | Notes |
|---|---|---|---|
| `tokenserver` | `eclipse-temurin:21-jdk` | `172.20.0.10` | runs `LocalTokenServer.java`; a port per node (`7011/7012/7013`) |
| `node1` | `cde-nfcassandra:latest` | `172.20.0.11`, rack `1a` | host cqlsh `7104`, nodetool/JMX `7501`; bootstrap seed |
| `node2` | `cde-nfcassandra:latest` | `172.20.0.12`, rack `1b` | host cqlsh `7105`, nodetool/JMX `7502` |
| `node3` | `cde-nfcassandra:latest` | `172.20.0.13`, rack `1c` | host cqlsh `7106`, nodetool/JMX `7503` |

---

## How it works

`bin/docker-entrypoint.sh` decides between **managed (DGW)** and **local** mode:

```
LOCAL_MODE = true   if /etc/nflx/environment is absent OR CASSANDRA_LOCAL_MODE=true
             false  otherwise   (unchanged production path)
```

| Concern | DGW (managed) | Local (`cassRun`) |
|---|---|---|
| Environment | `/etc/nflx/environment` provides `NETFLIX_*` | compose sets `NETFLIX_*` per node; entrypoint fills any gaps |
| Config | injected at `/var/lib/nflx-configs/out/cassandra` | rendered `local/<node>/conf` mounted at that path |
| Token / swappie calls | `metatron curl` (mTLS) to `odscasstokens` / `odscassswappie` | plain `curl` to `tokenserver:701N` |
| In-JVM `TokenService` | https + Metatron client cert | `http://…` URL → no SSL (`TokenService.getConnection`) |
| Which token a node gets | per-instance identity (Metatron / source) | the **port** it asks on (`701N` → node N) |
| Racks / DC | EC2 snitch | `GossipingPropertyFileSnitch` + generated `cassandra-rackdc.properties` |
| JMX | local-only (Metatron tooling) | `LOCAL_JMX=no`, no auth/ssl, `rmi.server.hostname=127.0.0.1`, port published 1:1 |

Per node, the entrypoint exports `JVM_EXTRA_OPTS` so the in-JVM `TokenService` (initial-token
lookup + seed provider) points at the same `http://tokenserver:701N`. Because the URL is `http://`,
`TokenService.getConnection` skips Metatron mTLS entirely — `https` (DGW) still uses Metatron.

`CASSANDRA_REQUIRE_ASSIGNED_TOKEN=true` stays on, so a node still refuses to self-assign random
tokens — it uses the token the local server hands out, exactly like prod. The cluster starts with
`auto_bootstrap: false` (fresh cluster, pre-assigned non-overlapping tokens): nodes claim their
tokens and form the ring via the seed list without streaming.

### Per-node token assignment by port

`/v1/cluster/{env}/{app}` returns all members on every port (so the seed provider sees the whole
cluster), but `/v1/token/current` returns a different token per port. Each node container is
pointed at its own port (`CASSANDRA_TOKEN_SERVICE_URL=http://tokenserver:701N`), so it receives
the right token without relying on source IP — Docker NATs container traffic, so source-IP
identification is unreliable.

---

## Connect from `./bin/`

Each node publishes its native transport (cqlsh) and JMX (nodetool) on a distinct `localhost`
port, so the repo's `./bin/` tools work directly from the host:

| Node | Rack | `./bin/cqlsh` (native) | `./bin/nodetool` (JMX) |
|---|---|---|---|
| node1 | 1a | `127.0.0.1 7104` | `-p 7501` |
| node2 | 1b | `127.0.0.1 7105` | `-p 7502` |
| node3 | 1c | `127.0.0.1 7106` | `-p 7503` |

```bash
./bin/nodetool -h 127.0.0.1 -p 7501 status     # node1 ring view
./bin/nodetool -h 127.0.0.1 -p 7502 info       # node2
./bin/cqlsh 127.0.0.1 7104                      # node1
./bin/cqlsh 127.0.0.1 7106                      # node3
```

JMX traverses Docker because local mode sets `LOCAL_JMX=no` (binds all interfaces) and
`java.rmi.server.hostname=127.0.0.1`, and each node's JMX port is mapped 1:1 — so the RMI stub the
client receives (`127.0.0.1:<jmxport>`) resolves back through the port mapping to the same node.
Auth and SSL are off locally.

A multi-rack keyspace replicates across the three racks:

```sql
CREATE KEYSPACE demo WITH replication = {'class':'NetworkTopologyStrategy','us-east-1':3};
```

> `./bin/nodetool` only needs a JDK (you have one). `./bin/cqlsh` runs the in-tree cqlsh and needs
> a Python 3 with the driver's deps — if you hit `ModuleNotFoundError: six.moves`, run
> `pip3 install six geomet`. If you'd rather not touch your host Python, use a throwaway client on
> the cluster network: `docker run --rm --network nfcassandra-cluster_cass cassandra:4.1 cqlsh 172.20.0.11 7104`.
> (The `cqlsh` **inside** the node image is broken on Mac-built images — see [Known limitations](#known-limitations).)

---

## Customizing

Everything is driven by the `localCluster` spec at the bottom of `build.gradle`:

```groovy
ext.localCluster = [
    name : 'nfcassandra-cluster', subnet: '172.20.0.0/16', tokenIp: '172.20.0.10',
    app  : 'cass_local', env: 'local', region: 'us-east-1',
    nodes: [
        [name:'node1', ip:'172.20.0.11', port:7011, token:'-9223372036854775808', az:'us-east-1a', rack:'1a'],
        [name:'node2', ip:'172.20.0.12', port:7012, token:'-3074457345618258603', az:'us-east-1b', rack:'1b'],
        [name:'node3', ip:'172.20.0.13', port:7013, token:'3074457345618258602',  az:'us-east-1c', rack:'1c'],
    ],
]
```

- **Node count / IPs / tokens / racks:** edit `nodes` and re-run `./gradlew cassRun`. Use balanced
  Murmur3 tokens (`-2^63 + i·2^64/n`) for an even ring.
- **Shared Cassandra settings** (timeouts, RF guardrails, snitch, …): edit `local/cass-template.yaml`.
- **Simulate the swappie assign flow:** add `--start-unassigned` to the `tokenserver` entrypoint
  args (in the generated `local/cluster-compose.yml`, or wire it into the token args in
  `build.gradle`). Then `/v1/token/current` returns `404` until the entrypoint's first
  `poke_swappie`, mimicking the production "boot → no token → poke → assigned → start" loop.

The per-node dirs and `local/cluster-compose.yml` are generated and git-ignored.

---

## Troubleshooting

- **A node loops on `No token assigned, skipping start`** — it can't reach its token port. Check
  the token server: `docker logs nfcass-tokenserver`, and from a node
  `docker exec nfcass-node2 sh -c 'curl -s http://tokenserver:7012/v1/token/current'`.
- **node2/node3 never start** — they wait for node1 to become healthy. Watch node1:
  `docker compose -f local/cluster-compose.yml logs -f node1`. Under emulation node1 can take a
  few minutes to open its native port.
- **Nodes don't see each other / "Unable to gossip with any seeds"** — confirm the token server's
  `--node ip=` values match the compose static IPs and the rendered `listen_address`. All three are
  derived from the `localCluster` spec, so re-running `./gradlew cassRun` realigns them.
- **`jibDockerBuild` can't pull the base image** — you need corp network access to
  `dockerregistry.test.netflix.net`.
- **`cassClean` leaves files behind** — data is written by the in-container root user; `cassClean`
  removes it from a throwaway container. If something is still locked, run `./gradlew cassStop` first.

---

## Known limitations

**Building on Apple Silicon / any non-linux-amd64 host.** The Jib image is `linux/amd64` (matching
the DGW base), so on an arm64 Mac the nodes run under emulation — the cluster is slower to converge,
but it works. The `stageJib*` Gradle tasks also `pip install` the Python `pylib` (used by the
in-container `cqlsh` for Metatron auth) **on the build host**, so a Mac build bakes macOS/arm64
native libs into the linux image; the in-container `cqlsh` then fails with
`ImportError: …/_rust.abi3.so: invalid ELF header`. Workarounds:

- Use an **external** `cqlsh` (see [Connect](#connect)) — recommended for local dev anyway.
- Build the image on **linux/amd64** (e.g. CI) for a working in-container `cqlsh`.

`nodetool`, the servers, and all CQL traffic are unaffected — only the in-container `cqlsh` helper
depends on that `pylib`.
