---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-14
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Authoritative source for the correctness guarantees TCM claims; each claim became a candidate property.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: 160 commits mined for historical TCM bug fixes; each past bug became a candidate property (regression-as-property).
---

# SUT Analysis — Transactional Cluster Metadata (TCM / CEP-21)

## Scope

The system under test is Apache Cassandra `trunk` (6.0), narrowed to the
**Transactional Cluster Metadata** subsystem: `src/java/org/apache/cassandra/tcm/`.

TCM replaces gossip-propagated, eventually-consistent cluster state (`TokenMetadata`,
schema pull/push, pending ranges) with a **linearized, replicated log of metadata
transformations**. Everything else in Cassandra (storage engine, compaction, CQL
execution, Accord) is in scope only insofar as it consumes cluster metadata.

## Architecture

### The core data object: `ClusterMetadata`

`ClusterMetadata` (`tcm/ClusterMetadata.java`) is an immutable snapshot of all
cluster-wide state, uniquely identified by an `Epoch`. Its components:

| Field | Purpose |
|---|---|
| `epoch` | Monotonic version counter; the log position that produced this metadata |
| `metadataIdentifier` | Identifies *which* CMS produced this log; guards against split-brain |
| `schema` (`DistributedSchema`) | All keyspaces/tables — DDL is a log entry, not a gossip push |
| `directory` (`Directory`) | Node identity, state, location, addressing (replaces `TokenMetadata`) |
| `tokenMap` (`TokenMap`) | Token → node ownership |
| `placements` (`DataPlacements`) | (keyspace RF, token range) → read/write replica sets |
| `lockedRanges` (`LockedRanges`) | Ranges reserved by an in-flight operation; the concurrency admission gate |
| `inProgressSequences` (`InProgressSequences`) | Multi-step operations mid-flight (join/leave/move/replace/CMS reconfig) |
| `cmsMembership` (`CMSMembership`) | Current CMS members (read/write sets during reconfiguration) |
| `consensusMigrationState`, `accordFastPath`, `accordStaleReplicas` | Accord (CEP-15) integration state |

Immutability is load-bearing: "local metadata is immutable, no updates in place or
requirements for locking" (CEP-21). Consumers never see a partially-applied update.

### The log and its ordering authority

- A subset of nodes forms the **CMS** (Cluster Metadata Service). CMS members
  linearize appends into `system_cluster_metadata.distributed_metadata_log`.
- `Transformation` is a pure function `ClusterMetadata -> Result` (`Success` with new
  metadata, or `Reject`). See `tcm/Transformation.java` and `tcm/transformations/`.
- Append is a Paxos CAS requiring the new entry's epoch to be **strictly consecutive**
  (`PaxosBackedProcessor`, `AbstractLocalProcessor`). `AtomicLongBackedProcessor` is the
  single-node test substitute.
- `Reject`s are *not* persisted; they are linearized via a read confirming the
  transformation ran against the highest epoch.
- After commit, the CMS **best-effort** broadcasts the `Entry` to all peers
  (`Replicator`). No retries — reliability comes from catch-up, not from delivery.

### Local application: `LocalLog`

`tcm/log/LocalLog.java` (982 lines) is the per-node consumer. Key mechanics
(`processPendingInternal`, lines 492–603):

- `pending` is a `ConcurrentSkipListMap<Entry, Boolean>` with a **custom comparator that
  prioritizes `FORCE_SNAPSHOT`** over other kinds, so a snapshot can jump gaps.
- An entry is enacted only if `pendingEntry.epoch.isDirectlyAfter(prev.epoch)` — **or**
  it is a `PRE_INITIALIZE_CMS` / `FORCE_SNAPSHOT`, which are explicitly allowed to skip.
  Note the comment at 513–517: `INITIALIZE_CMS` is deliberately *not* allowed to skip,
  because doing so creates an unresolvable gap at `Epoch.FIRST`.
- Publication is `committed.compareAndSet(prev, next)`; a CAS failure throws
  `IllegalStateException` because pre-commit listeners have already fired.
- Two existing Java `assert`s (lines 542, 544) already state the two central local
  invariants: entry epoch == resulting metadata epoch, and the new epoch directly
  follows the previous unless snapshot/pre-init. **These are the highest-value
  Antithesis instrumentation points in the subsystem** — they are already the
  author's own statement of the invariant, but `assert` is disabled in production
  builds and, when enabled, kills the process rather than reporting a property.
- `hasGaps()`, `highestPending()`, `waitForHighestConsecutive()` expose the gap state.
- Sync and Async variants exist; Async runs a dedicated `Interruptible` thread.

### Multi-step operations (range movements)

Metadata atomicity is insufficient for topology change, because data must stream while
reads and writes continue. `MultiStepOperation<CONTEXT>` (`tcm/MultiStepOperation.java`)
models the phased plan: `PrepareJoin` → `StartJoin` → `MidJoin` → `FinishJoin`, and
analogues for leave/move/replace (`tcm/sequences/`: `BootstrapAndJoin`,
`UnbootstrapAndLeave`, `Move`, `BootstrapAndReplace`, `ReconfigureCMS`).

Three mechanisms make this safe:

1. **`LockedRanges`** — `Prepare*` computes affected ranges and locks them.
   `LockedRanges.intersects(ranges)` rejects the prepare if they overlap an existing
   lock. This is the *only* thing permitting concurrent range movements; CEP-21:
   concurrent movements "may only be permitted where there is no overlap between the
   affected ranges."
2. **`ProgressBarrier`** — before each step, a majority of the owners of the affected
   ranges (the superset of pre- and post-state replicas) must acknowledge the epoch
   enacting the previous step. `ProgressBarrier.await()` starts at
   `progress_barrier_default_consistency_level` and **relaxes** down through
   `relaxConsistency()` (EACH_QUORUM → QUORUM → LOCAL_QUORUM → ONE → NODE_LOCAL) toward
   `progress_barrier_min_consistency_level`. Relaxation is a deliberate liveness/safety
   trade and a prime target for testing.
3. **No liveness assumptions between steps.** CEP-21: interrupted sequences are
   resumable — "cluster metadata simply holds pending states for any node to be
   executed." A node may crash arbitrarily often mid-sequence. Cancellation is
   operator-initiated (`CancelInProgressSequence`), never triggered by failure
   detection, because "each node's view of liveness is both subjective and transient."

### CMS membership reconfiguration

`ReconfigureCMS` moves CMS membership in two phases per node: add to the *write* set,
stream the whole log, then add to the *read* set, then Paxos-repair. CEP-21 argues
safety from bounded divergence: divergence "cannot grow larger than a single epoch," so
"any two read or write quorums will have overlap." `CancelCMSReconfiguration` and
`resumeReconfigureCms()` exist for stuck reconfigurations.

### Catch-up and snapshots

- Divergence is detected in the request path: every message carries the sender's epoch.
  A replica that knows the coordinator *could not* have seen a relevant schema or
  placement change throws `CoordinatorBehindException`; otherwise it asynchronously
  fetches (`TCM_FETCH_PEER_LOG_REQ`, then `TCM_FETCH_CMS_LOG_REQ`).
- `LogState` = optional snapshot + list of subsequent entries. A received snapshot is
  applied as a synthetic `ForceSnapshot` entry inserted at the head of `pending`.
- After collecting responses, the coordinator re-checks `DataPlacements` and verifies
  the responses still satisfy the requested consistency level.

### Startup / upgrade

`tcm/Startup.java` picks a mode; the normal path is `Vote` — initialize as non-CMS,
discover an existing CMS via seeds (`tcm/discovery/`), else vote to establish one.
Post-upgrade clusters run in a minimal-modification mode with **no CMS members** until
`nodetool cms initialize`. `CMSLookup` + `TCM_DISCOVER_SURVEY`/`TCM_DISCOVER_PEERS`
implement rediscovery when a CMS majority changes broadcast addresses concurrently
(commit `eb95b34199`).

## Concurrency model

| Site | Concurrency | Risk |
|---|---|---|
| `LocalLog.processPendingInternal` | Single-caller by contract, not by construction ("Implementations have to guarantee there can be no more than one caller") | Double-application; the `IllegalStateException` on CAS failure is the tripwire |
| `pending` buffer | Concurrent appends from Replicator, peer fetch, CMS fetch, startup replay | Out-of-order/duplicate entries; gap handling |
| Paxos append | Multiple CMS nodes racing to append epoch N+1 | Lost proposals → retry-or-reject decision |
| `Prepare*` admission | Concurrent operators submitting join/leave/move | Overlapping locked ranges |
| Progress barrier | Node liveness changes mid-barrier | CL relaxation choosing an unsafe quorum |
| Listener notification | `notifyPreCommit` before CAS, `notifyPostCommit` after | Prepared-statement invalidation races (`9bf1680b1f`, `1a6b8e0628`) |
| `EpochAwareDebounce` | Coalesces concurrent catch-up RPCs | Cancellation/shutdown leaks (`f9e2f1b219`) |

## Failure-prone areas (evidence: historical fixes)

Mined from `git log -- src/java/org/apache/cassandra/tcm` (160 commits). The recurring
themes, each with a real fix behind it:

| Theme | Evidence commits |
|---|---|
| **Log gaps break catch-up** | `44ee9d6167` "Unable to catch up TCM Log from peer with gaps in log sequence" (CASSANDRA-21455); `693eab8776` revert of FetchCMSLog/FetchPeerLog changes |
| **Intermediate state visible during replay** | `5d4bcc797a` "Avoid exposing intermediate state while replaying log during startup" (CASSANDRA-19384) |
| **CMS membership changing under a commit** | `6dc9ca99fa` "Retry if node leaves CMS while committing a transformation" (CASSANDRA-19872); `51e01a3862` Paxos repair on CMS membership change (CASSANDRA-20467) |
| **CMS quorum unavailable / unrecoverable** | `279c0527aa` "Allow CMS reconfiguration to work around DOWN nodes"; `eb95b34199` CMS rediscovery and recovery protocol; `4fb81ea483` abort failed `cms initialize` |
| **Sequence bookkeeping** | `80971709b9` "Properly set lastModifiedEpoch on multistep operations" (CASSANDRA-19538); `60fe2dc61d` InProgressSequences serialization version check; `63c6261856` `writePlacementAllSettled` reimplementation (CASSANDRA-19193) |
| **Rejection handling** | `3e6a551dba` "Catch up committing node on rejection" (CASSANDRA-19260); `802ce7f8b2` always send commit failures as messaging failures |
| **Derived local state drifting from metadata** | `32755cabfa` "Correctly update peers tables following replacement" (CASSANDRA-19782); `c484fc511a` tool to repair peers tables inconsistent with cluster metadata; `38512a469c` peers-v2 on IP swap |
| **Empty/absent placements pre-initialization** | `ec7794f20f` NPE when meta keyspace placements empty; `2bc24da841` allow empty placements when deserializing; `95aca49915` NPE during initialization abort |
| **Retry/timeout policy** | `4f49ca5e29` `retryIndefinitely` dangerous with `RemoteProcessor` (CASSANDRA-20059); `b1f30e94f5` longer timeout for long-running ops |
| **Schema-change side effects** | `9bf1680b1f` prepared-statement invalidation race (CASSANDRA-20116); `1a6b8e0628` invalidate on every table metadata change |

Every row above is a scenario Antithesis is well-suited to reach: they are all
partial-failure, timing, or concurrency bugs, not input-validation bugs.

## Observation surface (what a workload can actually see)

This determines what is checkable, so it is recorded precisely.

**Per-node, over CQL (`system_views` virtual tables, `db/virtual/`):**

- `system_views.cluster_metadata_log` — `epoch` (PK), `kind`, `transformation`,
  `entry_id`, `entry_time`. Read from the CMS at `QUORUM`, so it reflects the
  authoritative log, not the local one.
- `system_views.cluster_metadata_directory` — `node_id` (PK), `host_id`, `state`,
  `cassandra_version`, `serialization_version`, `rack`, `dc`, `broadcast_address`,
  `broadcast_port`, `local_address`, `local_port`, `native_address`, `native_port`,
  `tokens`, `multi_step_operation`. Read from **local** `ClusterMetadata.current()` —
  which is exactly what makes cross-node comparison meaningful.
- `system.peers_v2`, `system.local` — derived state that must agree with the directory.

**Per-node, over JMX (`org.apache.cassandra.tcm:type=CMSOperations`, `CMSOperationsMBean`):**

- `describeCMS()` → `MEMBERS`, `IS_MEMBER`, `NEEDS_RECONFIGURATION`, `SERVICE_STATE`,
  `IS_MIGRATING`, `EPOCH`, `LOCAL_PENDING` (pending buffer size), `COMMITS_PAUSED`,
  `REPLICATION_FACTOR`, `CMS_ID` (`metadataIdentifier`).
- `dumpLog(startEpoch, endEpoch)` → `Map<Long, Map<String,String>>` — **the local view of
  the log**, per node. This is the primitive that makes log-prefix agreement checkable.
- `dumpDirectory(includeTokens)` → per-node directory dump.
- Mutators usable as workload actions: `reconfigureCMS(rf)`, `reconfigureCMSStatus()`,
  `cancelReconfigureCms()`, `resumeReconfigureCms()`, `snapshotClusterMetadata()`,
  `cancelInProgressSequences(owner, kind)`, `unregisterLeftNodes(ids)`,
  `setCommitsPaused(bool)`, `initializeCMS(ignore)`, `abortInitialization(initiator)`.
- `StorageServiceMBean`: `joinRing()`, `decommission()`, `move(token)`, `assassinateEndpoint()`,
  `getOperationMode()`, `getSchemaVersion()`.

**Deliberately dangerous, available, and excluded from the workload:**
`unsafeRevertClusterMetadata(epoch)` and `unsafeLoadClusterMetadata(file)` are the
CEP-21 "escape hatch" with "a high degree of risk." They can manufacture states the
protocol is not required to survive, so calling them would produce false positives.
Excluded — see `deployment-topology.md`.

## Assumptions

- `trunk` at `3c0affbeeb` includes Accord (CEP-15) integrated with TCM. Accord is
  **not** the target; the workload keeps Accord-managed tables out of scope so Accord
  failures do not masquerade as TCM failures. Accord's own TCM coupling
  (`AccordMarkStale`, `ReconfigureAccordFastPath`, consensus migration) is noted as a
  future expansion, not covered now.
- The cluster is created directly in TCM mode (fresh cluster → CMS initialized at
  startup). The **gossip→TCM upgrade path** (`nodetool cms initialize` from a running
  gossip cluster) is a distinct and historically bug-dense area
  (`4318e74180`, `cdfce6b4ac`, `417bb21d2e`, `db94321d71`) but requires a mixed-version
  or gossip-mode deployment. Out of scope for this harness; flagged as the highest-value
  second harness.
- A single datacenter. `EACH_QUORUM` progress barriers and DC-aware placement are
  therefore only partially exercised.

## Open Questions

- Does `LocalLog.processPendingInternal`'s single-caller contract hold under every
  entry-arrival path (Replicator, peer fetch, CMS fetch, startup replay, snapshot
  insertion)? The comment asserts the requirement but the class does not enforce it.
  (needs human input — a maintainer can answer faster than an experiment.)
- `system_views.cluster_metadata_log` reads from the CMS at `QUORUM`; under partition it
  may be unavailable on a node that is otherwise healthy. Confirmed by reading
  `ClusterMetadataLogTable.log()`. The workload therefore uses JMX `dumpLog` for
  cross-node log comparison and treats the virtual table as a convenience only.
- Is `metadataIdentifier` guaranteed unique per CMS initialization, and is any code path
  able to produce two distinct identifiers in one cluster? (partial: read
  `describeCMS`/`CMS_ID` plumbing and `EMPTY_METADATA_IDENTIFIER = 0`; did not trace
  identifier generation to its source.)
