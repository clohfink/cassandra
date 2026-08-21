---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-19
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Minimum CMS/RF sizing constraints for reconfiguration and quorum-overlap reasoning.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: Historical bugs determine which operations the topology must be able to perform (replace, address change, restart).
---

# Deployment Topology

## Summary

**14 containers: 7 ring nodes + 3 join-spares + 3 cold spares + 1 workload.** Single datacenter,
`RF=3`, CMS `RF=3`, seven nodes joined at startup (bootstrapped **serially** — see below), three
held out of the ring as `join_ring=false` spares, and three **cold spares** for node replacement.

> **Cold spares (added 2026-08-20 for `r-node-replaced`).** `cassandra-11/12/13` run the node-agent
> with `NODE_AGENT_AUTOSTART=0`, so Cassandra never starts and their address is never registered
> until the replace-node driver boots one with `replace_address_first_boot=<victim>`. This is the
> only kind of node that can perform a real different-address `BootstrapAndReplace`: a
> `join_ring=false` spare pre-registers its own address and is rejected ("already exists"), and
> replace-same-address is just a re-bootstrap (BootstrapAndJoin), not the replace MSO. Cold spares are
> kept out of the workload's `CASSANDRA_NODES` (listed in `WORKLOAD_COLD_SPARES` instead) so they do
> not block `wait-ready` or the checkers; they are idle (no JVM) until consumed. Each replace consumes
> one and retires one ring node, keeping ring size constant — self-limiting to three per run.

> **Serial ring bootstrap (added after run `82fa2eda…`).** The seven ring nodes bootstrap one at a
> time via a linear `depends_on: service_healthy` chain, not in parallel. When all seven joined at
> once, several bootstraps locked overlapping token ranges simultaneously, TCM correctly rejected the
> colliding plans, and Cassandra's startup path aborts fatally on a rejected join (`exit code 3`) — so
> a ring node never came up and `setup_complete` never fired (run went `incomplete`, zero properties).
> Serializing the initial bootstrap forms the ring reliably; concurrency is exercised after setup by
> the workload joining the spares on disjoint arcs. Full detail in `existing-assertions.md`.

> **Why grown from the original 5 (3 ring + 2 spares).** After run `139be454…`, the only unmet
> property was `r-concurrent-multistep-operations` — two multi-step operations in flight at once. In
> a 3-node RF=3 ring this is **unreachable by construction**: every node's affected ranges cover
> most of the token space, so any two topology operations overlap and TCM correctly rejects/serialises
> the second (observed directly: a second concurrent join rejected because "it interacts with a range
> locked by Key{Epoch=23}"). The concurrency-admission *safety* (`newly locked ranges do not intersect
> existing locks`) was already exercised via that rejection — but the *happy path* of two DISJOINT
> operations running to completion at once needs a ring large enough to contain disjoint operations.
> Seven ring nodes make two joins landing on disjoint arcs achievable. This is sized for the
> `cassandra_oss` Antithesis runner; it does not fit a small local Docker VM (see "Local resource
> requirements" in `../README.md`).

The original 5-node compose is preserved at `/tmp/docker-compose.5node.bak` during this session and
in git history; the reasoning below about seeds, spares, the control agent, and pinned config all
carry over unchanged — only the node counts grew (3→7 ring, 2→3 spares).

```text
                      +---------------------------+
                      |  workload  (client)       |
                      |  test template + checkers |
                      +---------------------------+
                        |   |   |   |   |     CQL (9042) + JMX (7199)
        +---------------+   |   |   |   +----------------+
        v                   v   v   v                    v
+---------------+  +---------------+  +---------------+  +---------------+  +---------------+
| cassandra-1   |  | cassandra-2   |  | cassandra-3   |  | cassandra-4   |  | cassandra-5   |
| seed, CMS     |  | seed, CMS     |  | CMS           |  | spare         |  | spare         |
| in ring       |  | in ring       |  | in ring       |  | join_ring=off |  | join_ring=off |
+---------------+  +---------------+  +---------------+  +---------------+  +---------------+
        ^-------------------^------------------^------------------^------------------^
                     internode: gossip 7000, TCM verbs, streaming
```

Every Cassandra container additionally runs a small **node control agent** on port 7788 —
see "The node control agent" below for why it is necessary and why it is not a cheat.

## Container-by-container justification

Every container has to earn its place, because each one expands the state space Antithesis
must explore.

### `cassandra-1`, `cassandra-2`, `cassandra-3` — the steady cluster

Three is the floor, not a choice:

- **`RF=3` is required** for `b-replication-factor-never-under` and
  `b-progress-barrier-quorum-sound` to be meaningful. At `RF=1` there are no quorums, no
  progress-barrier majorities, and no replica sets to preserve — the entire Category B
  argument evaporates.
- **CMS `RF=3` is required** for `c-cms-reconfiguration-quorum-overlap`. Quorum overlap between
  read and write CMS sets is only a question when a quorum is larger than one node. CEP-21's
  worked example needs at least `{A,B,C}`.
- **Two seeds, not three.** `cassandra-3` is a non-seed CMS member so the harness covers the
  "learn about the CMS from a seed" path in `Startup`'s `Vote` mode rather than only the
  seed path.

### `cassandra-4`, `cassandra-5` — two spares, both necessary

This is the one place the topology spends a container on a specific property, so the
reasoning is recorded explicitly.

**Why any spare:** a join can only be tested if there is a node outside the ring to join. With
three nodes all in the ring, the only available membership operation is decommission, and
decommissioning below `RF=3` breaks every Category B property.

**Why a second spare:** `r-concurrent-multistep-operations` requires two multi-step operations
in flight simultaneously, and it guards `b-no-overlapping-locked-ranges` and
`b-locked-ranges-match-sequences` — TCM's headline capability over gossip. With one spare, the
only concurrent pair available is {join, decommission}, and a decommission concurrent with a
join in a 4-node cluster drops the ring to 3 with `RF=3`, which collides with
`b-replication-factor-never-under`'s operator-caused-under-replication regime. With two spares
the workload can run {join, join} on disjoint ranges while the ring stays at or above `RF`,
which is the clean case.

The fifth container is therefore bought specifically to make the concurrency properties
non-vacuous. If it turns out to be too expensive, the honest consequence is that
`r-concurrent-multistep-operations` will rarely fire and the four properties it guards become
uninformative — that is the trade, stated so it is not made by accident.

**How spares are held out:** started with `-Dcassandra.join_ring=false`. The node boots,
discovers the CMS, registers (obtaining a `NodeId` via `Register`), and follows the metadata
log — but does not bootstrap into the ring. This is a real, supported Cassandra mode, and it
has a bonus: a registered-not-joined node is exactly the `REGISTERED` directory state that
`d-peers-table-matches-directory` has to reason about, so the state exists from the first
second of every timeline rather than only during a join.

### `workload` — the client

One container. It holds the test template at `/opt/antithesis/test/v1/tcm/`, emits
`setup_complete` from its entrypoint once the cluster is healthy, and then stays alive so
Antithesis can run test commands in it.

Per the Antithesis test-command semantics, `setup_complete` is emitted by the **entrypoint**,
not by a `first_` command — `first_` commands only run *after* Antithesis has received the
readiness signal.

Keeping the workload in a single container is deliberate: it must hold cross-node state (the
per-node epoch high-water marks for `a-epoch-monotonic-per-node`, the submitted/acked tag
ledger for `c-commit-survives-cms-membership-change`) and comparing views across nodes requires
one process that can see all of them. Splitting the workload would mean sharing that state
between containers, which would need a side channel that faults could disrupt — turning
harness bookkeeping into a source of false failures.

## What is deliberately *not* here

| Not included | Why |
|---|---|
| A second datacenter | Doubles the container count. Buys `EACH_QUORUM` barriers and per-DC CMS RF (`reconfigureCMS(Map<String,Integer>)`). Recorded as a gap in `property-relationships.md`, not a first-harness need. |
| A gossip-mode / mixed-version cluster | The gossip→TCM upgrade path is the densest historical bug area, but needs different images and a different startup choreography. Separate harness. |
| Accord-enabled tables | Accord is integrated on `trunk` but excluded so failures stay attributable to TCM. See the assumption below. |
| Any external dependency | Cassandra has none. Nothing to mock, which is why there is no "dependencies" tier in the diagram. |
| More than 5 nodes | Each additional node multiplies the interleavings Antithesis must explore without unlocking a new property. Five is the smallest set that covers `RF=3` + CMS `RF=3` + two concurrent joins. |

## The node control agent

**The problem.** Antithesis node faults — including node termination — are **disabled by
default**; enabling them requires arrangement with a forward-deployed engineer. But three
things the property catalog depends on are restart-shaped:

- `a-epoch-monotonic-per-node`'s cross-restart case, which is the one CASSANDRA-19384 broke
  (`5d4bcc797a`, "Avoid exposing intermediate state while replaying log during startup").
- `b-sequence-resumable-after-crash`, where `TCM_implementation.md` explicitly invites killing
  the node "an arbitrary number of times during streaming."
- **Node replacement**, which cannot be done any other way: it requires restarting a process
  with `-Dcassandra.replace_address_first_boot=<addr>`. No JMX call can do this. Without a
  restart mechanism, `BootstrapAndReplace` is untestable, and with it goes the regression
  coverage for `32755cabfa` (peers tables after replacement) and `e5973bf34f` /
  `cbe07fd57e` (CMS reconfiguration around replacement).

**The design.** Each Cassandra container's entrypoint is a supervisor loop rather than the
Cassandra process directly. Alongside it runs a small Python 3 stdlib HTTP agent on port 7788
exposing: `stop`, `start` (with optional JVM flags), `restart`, `wipe-and-restart` (clear the
data directory to force a rebootstrap), and `status`. Python 3 is already present in the image
for `cqlsh`, so this adds no dependency.

**Why this is not cheating the fault model.** The agent is a *workload action*, not a fault
bypass. The distinction matters:

- It is reachable only over the network, on the same container network as everything else, so
  Antithesis's network faults apply to it. A partition can cut the workload off from the agent,
  and the workload must handle that — which is realistic, since it mirrors an operator losing
  access to a node.
- It performs only operations a real operator performs with `systemctl` and a config edit. It
  does not reach into Cassandra's internals or manipulate metadata.
- It is the mechanism, not the fault: Antithesis still decides the *timing* of everything
  around it, and thread pausing plus network faults still determine what state the node is in
  when the restart lands.

`init: true` is set on every service so the supervisor is not pid 1 and core dumps remain
possible.

## Configuration pinned for property soundness

These are harness configuration choices that a property's interpretation depends on. They are
recorded here because changing one silently changes what a property means.

| Setting | Value | Which property depends on it |
|---|---|---|
| `progress_barrier_min_consistency_level` | a quorum level | `b-progress-barrier-quorum-sound`. With a permissive minimum, reaching a sub-quorum barrier is an operator choice rather than a defect, and the assertion would report configuration as a bug. See the `(needs human input)` entry in that property's evidence file. This narrows `r-progress-barrier-relaxed` to one relaxation step — a trade documented in `r-progress-barrier-relaxed.md`. |
| `progress_barrier_default_consistency_level` | one level above the minimum | Leaves exactly one relaxation step available, so `r-progress-barrier-relaxed` can still fire. |
| `num_tokens` | small (e.g. 4) | `d-ring-fully-owned` reconstructs ranges from the token list on every check cycle; a large vnode count makes that expensive without adding coverage. Note vnodes (>1) make `nodetool move` unsupported, so range-movement concurrency is driven by concurrent *joins*, not moves — see `r-concurrent-multistep-operations.md`. |
| `allocate_tokens_for_local_replication_factor` | **disabled** (commented out) | Ships as `3` in `conf/cassandra.yaml`, which makes bootstrap pick tokens *deterministically*. Two spares bootstrapping at once then compute identical tokens and TCM rejects the second ("some tokens are already assigned") — so concurrent joins were impossible and `r-concurrent-multistep-operations` was never reached in run `8bf9b2c6…`. Disabling it reverts to random allocation, so simultaneous joins get disjoint tokens and are both admitted. The entrypoint comments it out. |
| transient replication | disabled | `b-replication-factor-never-under` and `d-ring-fully-owned` treat transient replicas differently; enabling it would make a failure ambiguous between two subsystems. See `d-ring-fully-owned.md`. |
| Accord | no Accord-enabled tables | Attributability — see assumptions. |
| `MAX_HEAP_SIZE` / `HEAP_NEWSIZE` | `512M` / `128M` per node | **Load-bearing, established empirically.** Unset, `cassandra-env.sh` sizes the heap at half of container-visible RAM — on a 7.7 GB Docker VM that is ~3.8 GB *per node*, so the nodes are SIGKILLed by the OOM killer. The symptom is `last_exit_code: -9` from the control agent and a container that reports unhealthy, i.e. it presents as "the node never came up" rather than as memory pressure. |
| `file_cache_size` / `networking_cache_size` / `memtable_heap_space` | `32MiB` / `32MiB` / `64MiB` | Off-heap defaults are sized for a dedicated host: the chunk cache alone is 512 MiB and networking 128 MiB *per node*, which is ~3.2 GB off-heap across five nodes before any heap. Capping these is as necessary as capping the heap. |
| `concurrent_reads` / `concurrent_writes` / `concurrent_counter_writes` | `8` each | Fewer request threads, fewer thread stacks. This harness drives a trickle of traffic, not a benchmark. |
| `NO_COLOR=1` | all containers | Antithesis stores raw bytes and does not render ANSI escapes; colour output is garbage in triage. |

## Instrumentation and cataloging

Per the Java SDK build requirements:

- Cassandra's jars are exposed at `/opt/antithesis/catalog/` in the node image, which enables
  **both** assertion cataloging (required for SDK assertions to work) and coverage
  instrumentation via bytecode weaving. No source or build changes are needed for coverage
  itself — only the SDK call sites need the dependency.
- Only one symlink level is followed, so the catalog directory contains the jars or a single
  symlink to a directory of jars, never a symlink to a symlink.
- Cassandra must be started with `-cp "/path/to/jars/*"` and a main class, **not** `java -jar`.
  Antithesis injects instrumentation dependency jars into the same directory as the application
  jar, and `java -jar` ignores `-cp`. This is a real constraint on the launch script:
  Cassandra's stock `bin/cassandra` already builds a classpath rather than using `-jar`, so it
  is compatible, but any wrapper must preserve that.
- Coverage instrumentation is also what enables **thread pausing**, which is the fault type
  most likely to find the `LocalLog.processPendingInternal` single-caller issue recorded as an
  open question in `a-no-gapped-metadata-published.md`.

## Fault exposure by property

Mapping the available fault types onto what the catalog needs, so gaps are visible:

| Fault | Default | Properties it drives |
|---|---|---|
| Network partitions | on | `a-log-prefix-agreement`, `c-cms-*`, `r-progress-barrier-relaxed`, `r-snapshot-catchup-used`, `r-coordinator-behind-rejection`, `e-*` |
| Congestion / latency | on | `c-commit-survives-cms-membership-change` (retry windows), `r-progress-barrier-relaxed` |
| Bad nodes (asymmetric) | on | `c-cms-membership-never-empty` (reconfiguration around unreachable members, `279c0527aa`) |
| Thread pausing | on **with instrumentation** | `a-epoch-monotonic-per-node`, `a-no-gapped-metadata-published` — the `LocalLog` CAS window |
| Node termination | **off** | Substituted by the control agent. Also note the docs' warning that restarted containers "may get new IP addresses and may lose non-durable filesystem state" — the new-address case is exactly `eb95b34199`'s CMS rediscovery scenario, so enabling this fault later would be high-value. |
| Clock jitter | off | Not currently needed by any property; TCM ordering is epoch-based, not time-based. Worth requesting later for the retry/timeout properties (`4f49ca5e29`, `b1f30e94f5`). |

## Assumptions

- **Accord stays inert without Accord-enabled tables.** The harness creates no Accord tables
  and does not enable consensus migration. If Accord turns out to be active regardless on
  `trunk`, its TCM-coupled sequences (`DropAccordTable`, `ReconfigureAccordFastPath`,
  `AccordMarkStale`) could produce in-progress sequences the workload did not initiate, which
  would confuse `b-locked-ranges-match-sequences` and `r-concurrent-multistep-operations`. This
  is the single most likely reason for early false positives and is the first thing to check if
  they appear.
- The cluster is created directly in TCM mode with the CMS initialized at startup, not upgraded
  from gossip.
- `system_views` virtual tables and the `CMSOperations` MBean are available on every node from
  startup, including on spares in `join_ring=false` mode. The MBean name is
  `org.apache.cassandra.tcm:type=CMSOperations`.
- The workload never calls `unsafeRevertClusterMetadata` or `unsafeLoadClusterMetadata`. These
  are CEP-21's escape hatch, which the document itself says "entails a high degree of risk";
  using them can manufacture states the protocol never promised to survive, producing findings
  that are not defects.

## Open Questions

- Do five Cassandra JVMs plus a workload fit comfortably in a single Antithesis timeline's
  resource envelope? **Partly answered locally (2026-08-14):** with default sizing they do *not* —
  five nodes on a 7.7 GB Docker VM were OOM-killed, because each auto-sized its heap to half of
  visible RAM and added ~640 MB of off-heap caches. With heap capped at 512 MB and the off-heap
  caches capped (see the configuration table above) the cluster comes up. Whether that leaves enough
  headroom under *fault injection* — where restarts and streaming add transient pressure — is still
  open, and the first thing to cut if not is `cassandra-5`, with the consequence for
  `r-concurrent-multistep-operations` spelled out above.
- Startup was originally a serial `depends_on` chain (each node waiting for the previous to be
  healthy), which made bring-up take roughly two minutes *per node* in sequence. Every node now
  waits on `cassandra-1` only. `cassandra-1` itself needs no dependency: it lists a not-yet-running
  peer as a seed, logs `Seed provider couldn't lookup host cassandra-2`, and after
  `discovery_timeout` (30s, `discovery_rounds=5`) initialises the CMS alone — observed committing
  `PreInitialize` at epoch 1, `INITIALIZE_CMS` at epoch 2, `UnsafeJoin` at epoch 3. So the apparent
  chicken-and-egg between the two seeds is not a deadlock.
- Does `-Dcassandra.join_ring=false` still register the node and follow the metadata log on
  `trunk`, or has TCM changed its meaning? The property design assumes register-but-don't-join.
  If it instead prevents registration entirely, spares contribute nothing until joined and the
  `REGISTERED`-state coverage claimed above is lost. Verifiable in the first local compose run.
- Can the workload reliably distinguish a node it cannot reach because of an injected partition
  from one that is down? Several checkers depend on "only assert over nodes that answered," and
  conflating the two would either mask failures or invent them. The control agent's `status`
  endpoint helps but is itself subject to partition.
