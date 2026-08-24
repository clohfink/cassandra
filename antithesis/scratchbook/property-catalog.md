---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-20
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Source of the claimed guarantees; properties A1–A4, B1–B3, C1–C2, D1 are direct restatements of CEP-21 text.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: 160 commits mined; properties A2, A3, B2, C3, D2, D3 are regression targets for specific fixed bugs.
---

# TCM Property Catalog

34 properties, post-evaluation: 16 `Always`, 3 `Always` inside quiet-period commands
(liveness questions expressed as safety assertions after faults stop — see those entries for
why), 2 `AlwaysOrUnreachable`, 2 `Unreachable`, 9 `Sometimes`. No `Reachable`: the catalog
consistently prefers `Sometimes` on a meaningful condition over marking a line as hit.
Added 2026-08-20: `a-metadata-serialization-round-trips` (Always) and `r-node-replaced` (Sometimes).
Added 2026-08-23: `r-sequence-cancelled` (Sometimes).

Each is a condition a workload or a SUT-side callsite can evaluate, not a goal.

> **Evaluated.** `evaluation/synthesis.md` records 19 findings across four lenses. Six
> properties were added (`c-initialization-abort-recoverable`, `d-prepared-statement-not-stale`,
> `a-log-processing-never-concurrent`, `a-log-processing-never-halts`,
> `e-cluster-serves-requests-during-churn`, `h-all-nodes-compared`) and eleven refinements
> applied — most importantly R1 (placements are read from JMX, not reconstructed from tokens),
> R2 (`b-locked-ranges-match-sequences` moved SUT-side because `LockedRanges` has no external
> observation surface), and R4 (`a-metadata-identifier-unique`'s fault scenario was unreachable
> as originally specified, because Antithesis injects no faults before `setup_complete`).
> One **bias** is unresolved and needs a human decision: the catalog tests the control plane
> rather than the data-path guarantee the control plane exists to provide. See `synthesis.md` B1. Priorities: **P0** = a violation is silent data loss or split brain; **P1** = a
violation is an operational outage or stuck cluster; **P2** = coverage/reachability
properties that tell us whether the workload is actually reaching the interesting states.

Naming: `A` = log & epoch integrity, `B` = range movement safety, `C` = CMS membership,
`D` = derived-state consistency, `E` = liveness & recovery, `R` = reachability.

## Category A — Log and epoch integrity

The log is the root of all TCM correctness: every other guarantee is derived from "all
nodes apply the same immutable, totally-ordered sequence of transformations." If A1–A4
hold, most of TCM works; if any fails, nothing downstream can be trusted.

### a-log-prefix-agreement — Nodes never disagree about the content of an epoch

| | |
|---|---|
| **Type** | Safety |
| **Property** | For any epoch E and any two nodes that have both applied E, the transformation recorded at E is identical on both. |
| **Invariant** | Workload-side `Assert.always`, message `"TCM log entries agree across nodes at the same epoch"`. **Not implemented via `dumpLog`:** that method delegates to `ClusterMetadataLogTable.log()`, which reads the distributed metadata table at `QUORUM`, so calling it on N nodes returns the CMS's one authoritative view N times and the comparison could never fail (resolved during implementation — see the evidence file). Implemented instead against genuinely per-node state: the checker groups nodes by reported epoch and asserts that all nodes at the same epoch return an identical `dumpDirectory`, which is backed by `ClusterMetadata.current()` on the node being asked. Since `ClusterMetadata` is a deterministic function of the log prefix, a directory disagreement within one epoch *is* a log disagreement. `Always` is correct because the log is claimed immutable; grouping by epoch means lag never produces a false failure. Covers the directory component; schema is covered by `d-schema-agreement-at-same-epoch`. Restoring the direct entry-by-entry comparison needs one new read-only JMX method exposing `LocalLog.getLocalEntries` — the highest-value observability change this harness would ask for. |
| **Antithesis Angle** | Network partitions split the CMS so different subsets serve `FetchCMSLog`/`FetchPeerLog`; partition healing then forces reconciliation. The interesting interleaving is a Paxos proposal lost mid-flight and re-proposed while a peer has already served the original entry to a third node. |
| **Why It Matters** | This is the foundation of CEP-21: "once an event is assigned a particular order in the log, this cannot be modified." A violation means two nodes compute different `ClusterMetadata` from the same epoch — divergent ring, divergent schema, silent data loss. |

**Open Questions:**

- Which `ClusterMetadata` components remain uncompared? The directory-based check covers identity,
  state, tokens and in-flight sequences, and `d-schema-agreement-at-same-epoch` covers schema — but
  `lockedRanges`, `consensusMigrationState`, and `extensions` are surfaced by neither, so a
  divergence confined to one of those would be missed.

### a-epoch-monotonic-per-node — A node's published epoch never goes backwards

| | |
|---|---|
| **Type** | Safety |
| **Property** | The epoch published by any single node is non-decreasing for the lifetime of that node's process, including across log replay at startup and across snapshot application. |
| **Invariant** | Two assertions, distinct messages. SUT-side `Assert.always` in `LocalLog.processPendingInternal` immediately after the successful `committed.compareAndSet(prev, next)`, message `"TCM published epoch is non-decreasing"`, checking `next.epoch.isEqualOrAfter(prev.epoch)`. Workload-side `Assert.always` message `"TCM epoch observed by client is non-decreasing per node"`, comparing each node's `describeCMS().EPOCH` against the highest value previously observed from that same node in this timeline. `Always` rather than `AlwaysOrUnreachable` because the CAS site runs on every enacted entry — the path is not optional. |
| **Antithesis Angle** | Thread pausing (enabled by default with instrumentation) can suspend the `LocalLog` async processor between `notifyPreCommit` and the CAS while a snapshot entry jumps the queue via the `FORCE_SNAPSHOT`-prioritising comparator. Container restarts re-enter `replayPersisted()`, which is the path CASSANDRA-19384 broke. |
| **Why It Matters** | Regression target for `5d4bcc797a` "Avoid exposing intermediate state while replaying log during startup" (CASSANDRA-19384). An epoch regression makes a node revoke a schema or ownership change it already served to clients. |

**Open Questions:**

- None.

### a-no-gapped-metadata-published — Metadata is only published from a gapless prefix

| | |
|---|---|
| **Type** | Safety |
| **Property** | A node never publishes `ClusterMetadata` built from a log with a hole in it; every enacted epoch either directly follows the previous one or arrives as a snapshot/pre-initialize entry that legitimately jumps. |
| **Invariant** | SUT-side `Assert.always` in `LocalLog.processPendingInternal` after the transform succeeds, message `"TCM enacted epoch directly follows previous or is a legal jump"`, with the condition `next.epoch.isDirectlyAfter(prev.epoch) || isSnapshot || kind == PRE_INITIALIZE_CMS`. This is a direct promotion of the existing Java `assert` at `LocalLog.java:544`. Paired with a second `Assert.always`, message `"TCM entry epoch matches resulting metadata epoch"`, promoting the `assert` at line 542. `Always` is right: both conditions are unconditional invariants of the enactment path, and the existing `assert`s are the maintainers' own statement of exactly that. |
| **Antithesis Angle** | Partition a node from the CMS so entries arrive out of order via peer fetch, then heal and let a snapshot arrive concurrently with the backfilled entries. The `FORCE_SNAPSHOT` comparator priority plus the "drop entries below the snapshot epoch" rule is precisely the logic under test, and it is the logic CASSANDRA-21455 found broken. |
| **Why It Matters** | Regression target for `44ee9d6167` "Unable to catch up TCM Log from peer with gaps in log sequence" (CASSANDRA-21455) and the `693eab8776` revert. The code comment at `LocalLog.java:513-517` documents an unresolvable-gap-at-`Epoch.FIRST` failure mode that the current guard is specifically shaped to avoid — exactly the kind of narrow guard worth re-attacking. |

**Open Questions:**

- Does the single-caller contract on `processPendingInternal` hold across all five entry-arrival paths (Replicator broadcast, peer fetch, CMS fetch, startup replay, snapshot insertion)? If it does not, the CAS-failure `IllegalStateException` at line 565 is reachable and this property should be split into a separate concurrency property. `(needs human input)`

### a-log-processing-never-concurrent — The log processor never runs concurrently with itself

| | |
|---|---|
| **Type** | Safety (impossible state) |
| **Property** | `LocalLog.processPendingInternal` is never entered concurrently, so the CAS that publishes new metadata never loses. |
| **Invariant** | SUT-side `Assert.unreachable` at `LocalLog.java:565` — the `else` branch that throws `IllegalStateException("CAS conflict while trying to commit entry...")` — message `"concurrent TCM log processing detected via CAS conflict"`. `Unreachable` is exactly right: the method's own doc comment declares this impossible ("Implementations have to guarantee there can be no more than one caller"), and the surrounding code comment says "Since we disallow concurrent calls to `processPendingInternal`". Reaching it means a documented internal invariant was violated. |
| **Antithesis Angle** | Thread pausing (available once the jars are instrumented) is the fault type that reaches this: suspend the `LocalLog` async processor between `notifyPreCommit` and the CAS while another entry-arrival path — Replicator broadcast, peer fetch, CMS fetch, startup replay, or snapshot insertion — tries to process. There are five arrival paths and the mutual exclusion is a contract on implementations rather than a construction, so this is a real hypothesis, not a formality. |
| **Why It Matters** | Two things. First, the failure is currently invisible: the throw is caught by `catch (Throwable t)` at line 575, logged as "Could not process the entry", and processing continues — so today a violated core invariant produces no test failure anywhere. Second, it converts the `(needs human input)` open question in `a-no-gapped-metadata-published` into an empirical answer, at the cost of one assertion in a file already being edited. |

**Open Questions:**

- If this fires, is the correct fix mutual exclusion inside `LocalLog` or a correction to whichever
  caller violated the contract? Answering needs the arrival path, so the assertion `Details` must
  include the entry kind and both epochs.

### a-log-processing-never-halts — A node never permanently stops applying log entries

| | |
|---|---|
| **Type** | Safety (impossible state) |
| **Property** | No node ever throws `StopProcessingException`, which would permanently halt its application of metadata log entries. |
| **Invariant** | SUT-side `Assert.unreachable` at both throw sites in `LocalLog.processPendingInternal` (`LocalLog.java:532` for a transform that threw, `:538` for a transform that returned a rejection), message `"TCM log processing halted on a transformation failure"`. `Unreachable` because both sites log that the condition "can mean that this node is configured differently from CMS" — impossible in a homogeneous harness where every node runs the same image and config. |
| **Antithesis Angle** | Reached if a transformation is deterministic on the CMS but not on a replaying peer — e.g. a transformation whose result depends on local state, or one applied against metadata that diverged. Partitions plus snapshot-based catch-up are what produce a peer applying an entry against a base state the CMS never had. |
| **Why It Matters** | A node that halts log processing is frozen at its current epoch forever while remaining up and serving traffic. It would eventually surface as an `e-cluster-converges-after-faults` failure, but with the root cause several steps removed and easily misread as a catch-up bug. Asserting at the throw site names the cause directly. Both throws are also inside the `catch (Throwable t)` region's blast radius, so today this is a log line, not a failure. |

**Open Questions:**

- Is there a legitimate operational reason for `StopProcessingException` in a heterogeneous
  cluster (mixed versions mid-upgrade) that would make `Unreachable` wrong there? This harness is
  homogeneous so the assertion is sound here, but an upgrade harness would need it relaxed to
  `AlwaysOrUnreachable` or removed. `(partial: confirmed both throw sites' log messages blame configuration divergence; the mixed-version case was not traced)`

### a-metadata-identifier-unique — One cluster never runs two metadata services

| | |
|---|---|
| **Type** | Safety |
| **Property** | All nodes in the cluster report the same non-zero `metadataIdentifier` once the CMS is initialized. |
| **Invariant** | Workload-side `Assert.always`, message `"all nodes report a single cluster metadata identifier"`, comparing `describeCMS().CMS_ID` across all reachable nodes and requiring cardinality 1 and value `!= 0` (`ClusterMetadata.EMPTY_METADATA_IDENTIFIER`). `Always` because two identifiers is split brain — there is no execution where it is acceptable. |
| **Antithesis Angle** | **(R4)** Not reachable at initial startup: Antithesis injects no faults before `setup_complete`, and the harness emits it only once the CMS is initialized. Reached instead by the workload's `wipe-and-rejoin` action — `unregisterLeftNodes` the node, wipe its data directory via the control agent, and restart it so it re-enters `Startup`/`Discovery` *during* the fault-injected phase, partitioned from the nodes that know the CMS. `Startup`'s `Vote` mode plus `Discovery` is the code that must refuse to form a second CMS. |
| **Why It Matters** | Two metadata identifiers means two independent linearized histories — the worst possible TCM failure, since both halves would consider themselves authoritative and accept conflicting DDL and ownership changes. |

**Open Questions:**

- Where is `metadataIdentifier` generated, and can two concurrent initializations produce different values rather than one rejecting? `(partial: confirmed the field exists on ClusterMetadata with EMPTY_METADATA_IDENTIFIER = 0 and is surfaced as CMS_ID in describeCMS; generation site not traced)`

### a-metadata-serialization-round-trips — Cluster metadata survives serialize→deserialize

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every published `ClusterMetadata` deserializes back to an equal object when round-tripped through `ClusterMetadata.serializer` at the cluster's current serialization version. A serializer/deserializer asymmetry is a correctness bug: metadata is serialized on every replication, commit response, and snapshot, so an asymmetry silently corrupts what peers and restarts reconstruct. |
| **Invariant** | SUT-side `Assert.always` in `LocalLog.processPendingInternal` inside the `committed.compareAndSet(prev,next)` block, message `"TCM cluster metadata survives a serialization round-trip"`. Condition (took two runs to get right — both naive checks false-positive): fail iff deserialize throws, OR the round-tripped object is unequal by `equals()` **AND** its re-serialized bytes differ (`faithful = equalsOk \|\| bytesOk`), at `Version.minCommonSerializationVersion()`. Rationale: `equals()` alone false-positives on CMS-reconfiguration sequences (transient field, run `efcbee84`); byte-stability alone false-positives on join sequences (order-unstable map, run `c3c05902`). Each benign asymmetry trips one signal; a real corruption trips both (verified locally: 0 both-fail across join + CMS-reconfig). Gated on `-Dcassandra.antithesis.serialization_check=true` (Antithesis node image only). `Always`: there is no epoch for which corrupting the on-wire form is acceptable. |
| **Antithesis Angle** | Fault injection drives the metadata through states plain tests rarely build — mid-sequence placements with locked ranges, multiple in-flight MSOs, CMS mid-reconfiguration, schema with column masks/types — exactly the shapes where a serializer edge case hides. The check runs on every committed epoch, so any such state is round-tripped the moment it is published. |
| **Why It Matters** | Regression target for a dense, still-active bug class: `1913eab974` "Fix deserialization of column masks in cluster metadata", `2bc24da841` "Allow empty placements when deserializing cluster metadata", `9af2b2cdf8` "Improve performance deserializing cluster metadata". These are found today by chance (a peer fails to catch up) epochs after the corruption; this turns them into an immediate, localized property failure. |

**Open Questions:**

- Should the round-trip also assert `serializedSize == actual bytes written` (a separate common serializer bug)? `(candidate refinement; not yet implemented)`

## Category B — Range movement safety

Multi-step operations are where metadata correctness meets data movement. These
properties test the three mechanisms CEP-21 relies on: locked ranges, progress barriers,
and resumability.

### b-no-overlapping-locked-ranges — Concurrent operations never touch the same ranges

| | |
|---|---|
| **Type** | Safety |
| **Property** | At no epoch do two distinct in-progress multi-step operations hold intersecting affected ranges. |
| **Invariant** | SUT-side `Assert.always` in `LockedRanges.lock(key, ranges)` before building the new map, message `"newly locked ranges do not intersect existing locks"`, asserting `intersects(ranges).equals(NOT_LOCKED)`. The **strict** form, with no same-key exemption: `lock()` builds via `ImmutableMap.Builder.build()`, which throws on a duplicate key, so a same-key re-lock is already impossible and any intersection found belongs to a different operation (resolved during implementation — see the evidence file). `Always` because CEP-21 makes non-overlap the *admission condition* for concurrency: a `Prepare*` that would overlap must be rejected, so reaching `lock()` with an overlap is already a bug. |
| **Antithesis Angle** | The workload submits join, decommission, and move concurrently from different nodes so their `Prepare*` transformations race for the same log position. Fault injection makes one proposal lose Paxos and retry, so it is validated against a metadata version it did not originally read — the classic stale-validation window. |
| **Why It Matters** | Overlapping movements mean two operations independently recompute placements for the same range, so one silently overwrites the other's replica set. The result is a range with the wrong replicas and no error anywhere — under-replication that surfaces later as data loss. |

**Open Questions:**

- Is `lock()` ever legitimately called with an overlapping key during sequence *advance* (as opposed to prepare), e.g. re-locking the same key for the same operation? If so the assertion must exclude same-key re-locks. `(partial: read LockedRanges.lock/unlock/intersects; lock() replaces by key so same-key re-lock is plausible, call sites in Prepare* not exhaustively enumerated)`

### b-locked-ranges-match-sequences — Locks are never orphaned

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every key in `lockedRanges` corresponds to a live in-progress sequence, and every in-progress sequence's ranges are locked. |
| **Invariant** | **(R2)** SUT-side `Assert.always`, message `"no locked range key survives removal of its sequence"`, in the transformation that removes a completed or cancelled sequence: the resulting metadata must contain no `LockedRanges` key belonging to the removed sequence. Moved SUT-side because `LockedRanges` is exposed by no virtual table and no `CMSOperationsMBean` method, so the workload cannot observe it; the original workload-side proxy (inferring orphaned locks from rejected prepares) was dropped rather than kept, since a weak check on a P0 property is worse than an honest absence. `Always` because an orphaned lock permanently blocks all future operations on that range. |
| **Antithesis Angle** | Kill the node owning a sequence between `Prepare*` and its first step; cancel a sequence via `cancelInProgressSequences` while a barrier is mid-flight. `MultiStepOperation.cancel()` is the unlock path and must run exactly once. |
| **Why It Matters** | Regression target for `80971709b9` "Properly set lastModifiedEpoch on multistep operations" (CASSANDRA-19538). An orphaned lock is an operational dead end: every subsequent bootstrap or decommission touching that range is rejected forever, with no obvious cause and no supported repair short of the unsafe metadata escape hatch. |

**Open Questions:**

- Does `CancelInProgressSequence` unlock ranges in the same transformation that removes the sequence, or in a follow-up? A two-step cancel has a window where the property is legitimately false. `(partial: confirmed transformations/CancelInProgressSequence.java and MultiStepOperation.cancel(ClusterMetadata) exist and return a Transformer; atomicity not confirmed)`

### b-replication-factor-never-under — Movements never under-replicate a range

| | |
|---|---|
| **Type** | Safety |
| **Property** | At every epoch, every token range of every user keyspace has at least RF write replicas, including at every intermediate step of an in-flight range movement. |
| **Invariant** | **(R1)** Workload-side `Assert.always`, message `"every range retains at least RF write replicas at every epoch"`. The checker reads `StorageServiceMBean.getRangeToEndpointWithPortMap(probeKeyspace)` and `getPendingRangeToEndpointWithPortMap(probeKeyspace)` per node — Cassandra's *own* computed replica sets — and asserts the union per range is `>= min(RF, liveRegisteredNodes)`. It does **not** reconstruct placements from tokens: doing so would reimplement Cassandra's placement algorithm in the workload and test the reimplementation. The `min` floor separates protocol-caused under-replication (the bug) from operator-caused (the workload decommissioned too far), and **(R9)** the workload additionally declines to submit operations that would drop the ring below RF. `Always`, because CEP-21 states committed changes must take effect "in a way that preserves advertised replication factor and quorum consistency" — this is unconditional, not best-effort. |
| **Antithesis Angle** | The phased design deliberately over-replicates during movement (writes go to both the old and new owner). The bug shape is a step ordering where the shrink happens before the grow. Faults that stall one step of a sequence while another sequence advances are exactly what exposes it. |
| **Why It Matters** | Regression target for `63c6261856` "Reimplement `ClusterMetadata::writePlacementAllSettled`" (CASSANDRA-19193) — the settled-placement calculation is what tells operators the movement is safe. Under-replication during a movement plus one node loss equals acknowledged-write loss. |

**Open Questions:**

- During `UnbootstrapAndLeave`'s final step the leaving node legitimately drops out; is there a defined instant where a range is at exactly RF versus RF-1? The property needs the precise boundary or it will false-positive. `(partial: read sequences/UnbootstrapAndLeave.java and LeaveStreams.java class structure; per-step placement deltas not computed)`
- What is the intended behaviour when RF exceeds the number of live nodes because the workload decommissioned too many? The assertion must exclude operator-caused under-replication from protocol-caused under-replication.

### b-sequence-resumable-after-crash — Interrupted operations always resume

| | |
|---|---|
| **Type** | Liveness |
| **Property** | A multi-step operation interrupted by node crash, restart, or partition eventually completes or is explicitly cancelled — it never becomes permanently stuck while the cluster is otherwise healthy. |
| **Invariant** | Workload-side `Assert.always` inside the `eventually_` recovery check, message `"all in-progress sequences drain after faults stop"`, asserting `inProgressSequences` is empty and `lockedRanges` is `EMPTY` on every node after a bounded recovery poll. Expressed as `Always` inside a quiet-period command rather than `Sometimes`, because after faults stop this is a required outcome, not an occasional one — `Sometimes` would pass on the one timeline that happens to drain and hide every timeline that does not. |
| **Antithesis Angle** | CEP-21 makes the strong claim that no liveness assumptions exist between steps: "the node may crash after executing `PrepareJoin` but before it updates tokens in the local keyspace." That is a direct invitation to kill the node in each of the four inter-step windows. Antithesis's ability to kill at an arbitrary instruction, then replay, is the only practical way to cover all of them. |
| **Why It Matters** | A stuck sequence holds locked ranges (see `b-locked-ranges-match-sequences`) and blocks all future topology change. This is the single most common operational TCM complaint shape, and `resumeReconfigureCms()` / `cancelInProgressSequences()` exist precisely because it happens. |

**Open Questions:**

- Does a sequence owned by a node that is *permanently* gone (killed and never restarted) count as "stuck"? The protocol requires operator cancellation there, so the workload must either restart every node it kills before the recovery check, or cancel on its behalf. `(partial: confirmed cancellation is operator-initiated by design per CEP-21 "liveness should never be a trigger for modifying cluster-wide state"; harness policy is to restart all killed nodes before the eventually_ check)`

### b-progress-barrier-quorum-sound — Barrier satisfaction always implies quorum overlap

| | |
|---|---|
| **Type** | Safety |
| **Property** | When a progress barrier is satisfied, the set of acknowledging nodes intersects every quorum of the affected replica group in both the pre-step and post-step placements. |
| **Invariant** | SUT-side `AlwaysOrUnreachable` in `ProgressBarrier.await(cl, metadata)` at the point of returning `true`, message `"satisfied progress barrier intersects pre- and post-step quorums"`, checking that the responded set satisfies `WaitFor.satisfiedBy` for both the read and write endpoint sets. `AlwaysOrUnreachable` rather than `Always` because a timeline may never advance a sequence far enough to evaluate a barrier at all, and "never ran" must not be a failure. |
| **Antithesis Angle** | The barrier *relaxes* consistency from the configured default down toward the minimum (`relaxConsistency()`: EACH_QUORUM → QUORUM → LOCAL_QUORUM → ONE → NODE_LOCAL) when nodes do not respond. Antithesis-induced partitions are what drive relaxation, so this property tests whether the relaxed level is still sound — the deliberate liveness/safety trade in the design. |
| **Why It Matters** | This is the CEP-21 headline safety theorem: a lagging coordinator "will **not** be able to collect a quorum for read or write that is inconsistent with a quorum obtained using metadata that is up to date." The barrier is the mechanism that makes it true. If relaxation to `ONE` or `NODE_LOCAL` breaks the overlap, the theorem fails and stale coordinators can serve inconsistent quorums. |

**Open Questions:**

- Is relaxation to `NODE_LOCAL` ever *intended* to be sound, or is it an explicit "operator accepted the risk" escape? If the latter, the assertion must be conditioned on the configured minimum level rather than asserting soundness unconditionally. `(needs human input)`

## Category C — CMS membership

### c-cms-membership-never-empty — The cluster never loses its metadata service

| | |
|---|---|
| **Type** | Safety |
| **Property** | Once the CMS is initialized (epoch >= `Epoch.FIRST`), the full CMS member set is never empty in any node's view. |
| **Invariant** | Workload-side `Assert.always`, message `"CMS member set is non-empty after initialization"`, asserting `describeCMS().MEMBERS` is non-empty on every reachable node whose reported `EPOCH >= 1`. `Always`; the epoch guard excludes the legitimate pre-initialization and post-upgrade states where CEP-21 says "the CMS has no members." **(R9)** The workload declines to submit a `reconfigureCMS(rf)` whose `rf` exceeds the live registered node count, and logs when it declines — otherwise an operator-caused rejection would generate noise against a P0 assertion, and noisy P0 assertions get weakened. |
| **Antithesis Angle** | Reconfigure the CMS while partitioning the members being removed, and cancel a reconfiguration mid-flight. `ReconfigureCMS` moves members through write-set-then-read-set phases; an interleaving that empties both sets is the target. |
| **Why It Matters** | An empty CMS means no metadata change can ever be committed again. Recovery requires the unsafe escape hatch that CEP-21 itself flags as carrying "a high degree of risk." Regression target for `279c0527aa` "Allow CMS reconfiguration to work around DOWN nodes" and `eb95b34199` CMS rediscovery. |

**Open Questions:**

- None.

### c-cms-reconfiguration-quorum-overlap — CMS read and write sets always intersect

| | |
|---|---|
| **Type** | Safety |
| **Property** | At every epoch during a CMS reconfiguration, any quorum of the CMS read set intersects any quorum of the CMS write set. |
| **Invariant** | SUT-side `AlwaysOrUnreachable` in `ReconfigureCMS.advance(context)`, message `"CMS read and write quorums intersect during reconfiguration"`, computing the read and write member sets from `CMSMembership` and asserting that `|read| + |write| - |read ∪ write| >= 1` for their quorum sizes. `AlwaysOrUnreachable` because reconfiguration is an optional path that many timelines will not enter. |
| **Antithesis Angle** | CEP-21's safety argument is that divergence "cannot grow larger than a single epoch," which is a *timing* claim — precisely what a deterministic simulator can falsify. Partitioning the joining CMS node during log streaming, between the write-set and read-set steps, is the shape to hit. |
| **Why It Matters** | Non-overlapping CMS quorums means two disjoint sets of nodes can each linearize an append, producing two divergent log tails at the same epoch. That is `a-log-prefix-agreement` failing at its root cause. Regression target for `51e01a3862` (CASSANDRA-20467), which added Paxos repair on CMS membership change because Paxos state, not just data, had to be current. |

**Open Questions:**

- Does `CMSMembership` expose distinct read and write sets at all points during reconfiguration, or is the distinction only implicit in `DataPlacements` for the metadata keyspace? The assertion's data source depends on the answer. `(partial: confirmed CMSMembership is a first-class ClusterMetadata field added by e1e56e5d5d "Add CMS membership directly to ClusterMetadata"; read/write set accessors not enumerated)`

### c-commit-survives-cms-membership-change — Commits are not lost when the CMS moves under them

| | |
|---|---|
| **Type** | Safety |
| **Property** | A transformation submitted while CMS membership is changing is either committed exactly once or cleanly rejected — never silently dropped, and never applied twice. |
| **Invariant** | Workload-side `Assert.always`, message `"every committed transformation appears exactly once in the log"`. The workload commits uniquely-tagged DDL (distinctly-named tables; `CustomTransformation` has no confirmed client submission path), maintaining a ledger of submitted / acked / unknown outcomes, then asserts: every **acked** tag appears exactly once, every **unknown** tag appears zero or one times, and no tag appears twice. **(R8)** The same ledger carries the rejection direction as a sub-condition — a tag whose commit was **rejected** must appear at no epoch, since `TCM_implementation.md` states "`Reject`s are not persisted in the log." A timeout is recorded as unknown, never as failed; collapsing the two is what turns this property into a false-positive generator under partition. `Always` — at-most-once and at-least-once are both unconditional. |
| **Antithesis Angle** | Reconfigure the CMS while the workload is committing continuously, and partition the node that served the commit request just after it appended but before it responded. `RemoteProcessor`'s retry against a *different* CMS node is then asked to decide whether its earlier attempt landed. |
| **Why It Matters** | Regression target for `6dc9ca99fa` "Retry if node leaves CMS while committing a transformation" (CASSANDRA-19872) and `4f49ca5e29` (CASSANDRA-20059, `retryIndefinitely` interacting badly with `RemoteProcessor` deadlines). A double-applied transformation corrupts metadata; a silently dropped one leaves an operator believing a change took effect when it did not. |

**Open Questions:**

- Is `CustomTransformation` a supported way to inject uniquely-tagged no-op entries into the log from a client, and does it survive a round trip intact? If not, the workload must tag via schema DDL (e.g. distinctly-named tables) instead, which is heavier but always available. `(partial: confirmed transformations/CustomTransformation.java exists and is a Transformation.Kind; client-side submission path not traced)`

### c-initialization-abort-recoverable — A failed CMS initialization leaves a re-initializable cluster

| | |
|---|---|
| **Type** | Safety |
| **Property** | After a CMS initialization is aborted, no node holds partial CMS state: either the CMS is fully initialized, or every node reports an uninitialized CMS that can be initialized again. |
| **Invariant** | Workload-side `Assert.always`, message `"aborted CMS initialization leaves no partial CMS state"`, asserting that across all reachable nodes the set of `(CMS_ID, EPOCH >= 1, MEMBERS non-empty)` triples is uniform — all initialized or all not — with no node reporting a non-zero `CMS_ID` while others report `0`. `Always`: a mixed state is unrecoverable without the unsafe escape hatch, so there is no execution in which it is acceptable. |
| **Antithesis Angle** | `initializeCMS(List<String> ignore)` proceeds without unanimous participation, which is exactly the condition under which an abort can leave some nodes ahead of others. Partition a subset mid-initialization, call `abortInitialization(initiator)`, heal, and attempt initialization again. Reaching this requires the workload's `wipe-and-rejoin` action (see `a-metadata-identifier-unique`) since initial startup is fault-free. |
| **Why It Matters** | Fills the catalog's blind spot around the pre-initialization window — every other Category A and C property guards itself out of it with `epoch >= FIRST`. Three fixed bugs lived there: `ec7794f20f` (NPE when meta keyspace placements are empty before CMS initialization), `2bc24da841` (allow empty placements when deserializing), `95aca49915` (NPE during initialization abort). The operator surface built around it — `4fb81ea483`'s abort command, the `ignore` list, `f05b27502f` "Improve CMS initialization" — is evidence this state machine is exercised in the field. |

**Open Questions:**

- Can a node that participated in an aborted initialization be re-initialized without a data wipe,
  or is a wipe required? This determines whether the property is about *recoverability* or merely
  about *uniformity*, and whether the workload's abort action needs a wipe to follow it.
- Does `abortInitialization(String initiator)` require the same initiator that called
  `initializeCMS`, and what happens if that node is unreachable? An abort that only the
  now-partitioned initiator can perform would be a liveness trap worth its own property.

## Category D — Derived state consistency

TCM is only useful if the state derived from it agrees with it. Historically this is
where the bugs actually shipped.

### d-schema-agreement-at-same-epoch — Same epoch implies same schema

| | |
|---|---|
| **Type** | Safety |
| **Property** | Any two nodes reporting the same epoch report the same schema version. |
| **Invariant** | Workload-side `Assert.always`, message `"nodes at the same epoch report the same schema version"`, grouping nodes by `describeCMS().EPOCH` and asserting each group has a single distinct `StorageServiceMBean.getSchemaVersion()`. `Always` because `ClusterMetadata` is a deterministic function of the log prefix — same epoch must mean same schema, with no timing excuse. |
| **Antithesis Angle** | Concurrent DDL from multiple coordinators during a partition, so different nodes learn the same epoch via different routes (Replicator broadcast vs. peer fetch vs. snapshot). |
| **Why It Matters** | The whole point of moving schema into the log was to eliminate schema disagreement. If two nodes at the same epoch disagree, either the transformation is not deterministic or the epoch is being published before the schema is applied. Related fixes: `9bf1680b1f` prepared-statement invalidation race (CASSANDRA-20116), `1a6b8e0628`. |

**Open Questions:**

- Is `getSchemaVersion()` computed from `ClusterMetadata.schema` synchronously with epoch publication, or updated by a listener that may lag? A lagging listener makes this property false transiently and the check would need to tolerate a window — which would substantially weaken it. `(partial: confirmed DistributedSchema is a ClusterMetadata field and that notifyPostCommit fires listeners after the CAS in LocalLog; the schema-version computation path was not traced)`

### d-peers-table-matches-directory — Derived peer state agrees with metadata

| | |
|---|---|
| **Type** | Safety |
| **Property** | Each node's `system.peers_v2` contents agree with the `Directory` component of that node's own `ClusterMetadata` for every peer in a settled state. |
| **Invariant** | Workload-side `Assert.always`, message `"system.peers_v2 agrees with the cluster metadata directory"`, comparing `SELECT peer, host_id, tokens FROM system.peers_v2` against `cluster_metadata_directory` on the same node, restricted to nodes not currently mid-sequence. `Always`, scoped to settled peers so in-flight movements do not false-positive. |
| **Antithesis Angle** | Replace a node, then partition the replacing node during the sequence; swap broadcast addresses. The historical bugs all involved the *transition* not being reflected in the derived table. |
| **Why It Matters** | Direct regression target for `32755cabfa` "Correctly update peers tables following replacement" (CASSANDRA-19782), `38512a469c` (peers-v2 on IP swap), and `c484fc511a`, which shipped a *repair tool* for peers tables inconsistent with cluster metadata — proof this drifts in the field. Drivers read `peers_v2` for token-aware routing, so drift sends client traffic to the wrong replicas. |

**Open Questions:**

- Which node states legitimately have no `peers_v2` row (`LEFT`, `REGISTERED`, `BOOTSTRAPPING`)? The exclusion set must be exact or the check is either noisy or vacuous. `(partial: confirmed Directory tracks NodeState and that cluster_metadata_directory exposes a `state` column; the peers_v2 write path's state filter was not read)`

### d-prepared-statement-not-stale — A prepared statement never executes against a stale table definition

| | |
|---|---|
| **Type** | Safety |
| **Property** | After a schema change is enacted on a node, a prepared statement re-executed on that node either runs against the new table definition or fails cleanly — it never returns results shaped by the pre-change definition. |
| **Invariant** | Workload-side `Assert.always`, message `"a prepared statement never returns results shaped by a stale table definition"`. The workload prepares a `SELECT` against a probe table, alters the table (adding a column with a known value), waits for the node's epoch to advance past the alter, then re-executes on that node's single-host session and asserts the result metadata reflects the new definition or the execution failed with an unprepared/invalid error. `Always`: silently serving the old shape is a wrong answer, and there is no execution in which that is acceptable. |
| **Antithesis Angle** | The seam is the ordering inside `LocalLog.processPendingInternal`: `notifyPreCommit` fires *before* the `committed.compareAndSet` at line 555 and `notifyPostCommit` *after*. Anything a post-commit listener maintains — including prepared-statement invalidation — lags the published epoch. The window is normally sub-millisecond; thread pausing widens it arbitrarily, and partitions vary which node learns the change first. |
| **Why It Matters** | Three fixed bugs on exactly this seam: `9bf1680b1f` "Avoid prepared statement invalidation race when committing schema changes" (CASSANDRA-20116), `1a6b8e0628` "Invalidate affected prepared stmts on every table metadata change", `740879d5a0` "Don't clear prepared statement cache on nodetool cms initialize". It is also one of the few client-visible wrong-answer paths in the catalog, which partially mitigates the control-plane bias recorded in `evaluation/synthesis.md` B1. |

**Open Questions:**

- Is invalidation performed in `notifyPreCommit` or `notifyPostCommit`? If pre-commit, the property
  holds by construction and this is a cheap regression guard; if post-commit, there is a real
  window and the property is a live hypothesis. This is the same unresolved question as in
  `d-schema-agreement-at-same-epoch`, and one code read answers both. `(partial: confirmed the pre/post ordering around the CAS in LocalLog:553-569; which hook invalidation uses was not traced)`
- Does the Java driver transparently re-prepare on receiving an unprepared error, and would that
  mask a violation? The workload must disable or detect re-preparation, or the check could pass on a
  silently-corrected result.

### d-ring-fully-owned — The token ring is always completely and unambiguously owned

| | |
|---|---|
| **Type** | Safety |
| **Property** | At every epoch, the union of token ranges in each keyspace's placements covers the entire ring with no gaps and no range claimed by two disjoint replica sets. |
| **Invariant** | **(R1)** Workload-side `Assert.always`, message `"placements cover the whole ring with no gaps"`, taking the key set of `StorageServiceMBean.getRangeToEndpointWithPortMap(probeKeyspace)` per node and asserting the ranges are contiguous and wrap exactly once. Reading Cassandra's own range map removes all token arithmetic from the workload. **(R5)** The checker records whether `inProgressSequences` was non-empty at evaluation time, and the run reports churn-vs-settled evaluation counts — as a static check on a settled ring this property is largely unit-testable, so its Antithesis value comes entirely from evaluations during interrupted concurrent movements. `Always` — a gap is unowned data. |
| **Antithesis Angle** | Concurrent `move` and `decommission` on adjacent token ranges, interrupted mid-sequence. Range merging after decommission is called "essentially an optimisation" in CEP-21, which is exactly the kind of code that gets an off-by-one at a range boundary. |
| **Why It Matters** | An unowned range means reads return nothing and writes are rejected or silently dropped for a slice of the keyspace. A doubly-owned range means two replica sets independently accept writes for the same keys. |

**Open Questions:**

- Do transient replicas or the `MetaStrategy` metadata keyspace (all CMS members own MIN→MAX) need to be excluded from the contiguity check? `(partial: confirmed MetaStrategy.partitioner is used for cluster_metadata_log and that CEP-21 states all CMS nodes own the full range; transient replication interaction not examined)`

## Category E — Liveness and recovery

### e-cluster-converges-after-faults — Everyone ends up at the same epoch

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After fault injection stops, all live nodes converge on the same epoch and byte-identical `ClusterMetadata`. |
| **Invariant** | `Assert.always` inside the `eventually_` command, message `"all live nodes converge to an identical cluster metadata epoch"`, polling `describeCMS().EPOCH` and the directory dump until stable or a bounded deadline, then asserting a single distinct epoch and a single distinct directory across all nodes. `Always` inside a quiet-period command: after faults stop, convergence is required, so a `Sometimes` here would hide every non-converging timeline. |
| **Antithesis Angle** | This is the payoff property for the whole harness. Antithesis's `eventually_` semantics stop all faults and restore killed containers, which is the only clean way to distinguish "still catching up" from "permanently divergent." |
| **Why It Matters** | Non-convergence after healing means catch-up is broken — a node is stuck behind forever, serving stale ownership and schema. This is the failure mode CASSANDRA-21455 produced: a gap the node could never resolve. |

**Open Questions:**

- How long is "eventually"? Catch-up after a long partition involves snapshot transfer and possibly streaming; too short a deadline reports a false violation. The harness starts with a generous bound and tightens it once real timings are observed. `(partial: confirmed tcm_await_timeout / tcm_rpc_timeout govern individual RPCs per TCM_implementation.md, but end-to-end convergence has no documented bound)`

### e-cms-accepts-commits-after-recovery — The metadata service comes back

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After faults stop, the CMS can commit a new transformation. |
| **Invariant** | `Assert.always` inside the `eventually_` command, message `"CMS accepts a new transformation after recovery"`, executing a real DDL statement and asserting it commits within a bounded retry window and that the resulting epoch is visible on all nodes. `Always` in the quiet period for the same reason as above. |
| **Antithesis Angle** | Kill and restore a CMS majority; change broadcast addresses concurrently. `eb95b34199` added `TCM_DISCOVER_SURVEY`/`TCM_DISCOVER_PEERS` specifically because a concurrently-readdressed CMS majority could not re-form a quorum. |
| **Why It Matters** | A cluster that cannot commit metadata changes cannot be operated: no DDL, no scaling, no node replacement. It stays up serving reads and writes, which makes it a silent operational trap rather than an obvious outage. |

**Open Questions:**

- None.

### e-cluster-serves-requests-during-churn — The cluster stays usable while metadata changes

| | |
|---|---|
| **Type** | Liveness |
| **Property** | During the fault-injected driver phase, it is possible to complete a read and a write against the probe keyspace at `QUORUM`. |
| **Invariant** | Workload-side `Assert.sometimes` in the `anytime_` checker, message `"a QUORUM read and write completed during churn"`, condition set when a probe read and a probe write both succeed within one check cycle. `Sometimes` rather than `Always`: individual requests are *expected* to fail under partition — asserting every request succeeds would be asserting the absence of the faults. The meaningful claim is that the cluster is not *continuously* unusable. |
| **Antithesis Angle** | Inverts the usual direction: rather than using faults to break an invariant, this asks whether the system remains usable *despite* them. The Antithesis test-command reference names this as a canonical `anytime_` use — "Availability monitoring: 'it's possible to make a read without timing out'." |
| **Why It Matters** | Every other safety property in this catalog is of the form "nothing bad is in the metadata," and the `e-*` recovery properties only check state *after* faults stop. A TCM bug that left every coordinator unable to construct a replica plan — so the cluster rejected every request for the entire driver phase and recovered cleanly at the end — would violate nothing else in the catalog. This closes that hole for a few lines in a checker the workload already runs. |

**Open Questions:**

- What is the right granularity: "at least once per run" (weak but never noisy) or "at least once
  per N check cycles" (stronger, and detects long unusable stretches)? Starting with per-run because
  it cannot false-positive; the stronger form needs observed baseline availability to calibrate.

## Category R — Reachability (workload effectiveness)

These do not find bugs directly. They answer "is the workload actually reaching the
states the safety properties are about?" Without them, all-green is uninformative.

### r-concurrent-multistep-operations — Two range movements really do overlap in time

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, two or more multi-step operations are in flight simultaneously. |
| **Invariant** | **SUT-side** `Assert.sometimes` in `LocalLog.processPendingInternal` inside the `committed.compareAndSet(prev,next)` block, message `"two or more multi-step operations were in flight at once"`, condition `next.inProgressSequences.size() >= 2`. Moved SUT-side after run `f863fad2` (2026-08-19): the workload's JMX poll observed only `peak_in_flight=1` because the overlap opens and closes between samples; checking at the metadata-publication point catches the transient regardless of poll cadence. The `serial_driver_concurrent_movements` command is the driver that makes it reachable; its `peak_in_flight` is now telemetry only. Confirmed firing (`ex=160`) in run `036a6fc3`. |
| **Antithesis Angle** | If this never fires, `b-no-overlapping-locked-ranges` and the entire concurrency-admission argument are untested, and the run's green result on those properties is meaningless. |
| **Why It Matters** | Concurrent disjoint-range movements are the headline capability TCM added over gossip. This property is the guard that we are testing it. |

**Open Questions:**

- None.

### r-node-replaced — Node replacement runs end-to-end

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, a node completes a replacement — a `FINISH_REPLACE` transformation is enacted. |
| **Invariant** | SUT-side `Assert.sometimes` in `LocalLog.processPendingInternal` inside the CAS block, message `"a node completed a replacement (FINISH_REPLACE enacted)"`, condition `kind == Transformation.Kind.FINISH_REPLACE`. Driven by `serial_driver_replace_node`: kill a **non-seed, non-CMS** ring node, wait for peers to mark it down, then boot a **cold spare** (`cassandra-11/12/13`, `NODE_AGENT_AUTOSTART=0` so its address is never registered) with `replace_address_first_boot=<victim>` — a genuine different-address `BootstrapAndReplace`. Two dead ends were ruled out by local smoke first: replace-same-address is a re-bootstrap (BootstrapAndJoin, never FINISH_REPLACE), and a pre-registered `join_ring=false` spare can't replace ("address already exists"). CMS members are excluded (a wiped member can't fetch the log to rebuild). SUT-side so it is reliable regardless of the driver's polling and fires for any replace, however initiated. Confirmed enacting PrepareReplace/StartReplace/MidReplace/FinishReplace in the local smoke. |
| **Antithesis Angle** | Replacement is a distinct multi-step operation (`PREPARE/START/MID/FINISH_REPLACE`) with its own locked-range and streaming shape. Making it reachable re-runs every range-movement safety invariant (`b-*`), the progress-barrier soundness check, and the serialization round-trip over the replace path — under partitions and thread pauses that stall streaming and barriers mid-replace. |
| **Why It Matters** | Replace-address is one of the densest range-movement code paths and was untested by the join/leave drivers. A replace that streams the wrong ranges, orphans a lock, or leaves the ring under-replicated is a data-loss bug; this makes the path get exercised so the safety properties can catch it. |

**Open Questions:**

- Should we also exercise replacing a CMS member (a distinct, more delicate scenario)? `(deliberately excluded today; worth a dedicated future driver)`

### r-sequence-cancelled — Aborting an in-flight operation rolls it back

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, an in-progress multi-step operation is cancelled — a `CANCEL_SEQUENCE` transformation is enacted — and the rollback leaves no orphaned lock or half-applied movement. |
| **Invariant** | SUT-side `Assert.sometimes` in `LocalLog.processPendingInternal` inside the CAS block, message `"an in-progress sequence was cancelled (CANCEL_SEQUENCE enacted)"`, condition `kind == Transformation.Kind.CANCEL_SEQUENCE`. Driven by `serial_driver_abort_sequence`: start a spare joining, kill it mid-bootstrap (which stalls the sequence and makes it abortable — the SUT rejects aborting a live node), wait for peers to mark it down, then `abortBootstrap`, which commits `CancelInProgressSequence` + `Unregister`. The rollback correctness itself is covered by the existing always-on `b-locked-ranges-match-sequences` and `b-no-overlapping-locked-ranges` (an orphaned lock after cancel trips them on the CANCEL_SEQUENCE transition); this property just makes that path reachable. SUT-side so it fires for any cancel, however initiated. |
| **Antithesis Angle** | Cancellation is the rollback half of every range movement and is only reached on the failure path (a bootstrap that got stuck under a partition, then aborted). Under fault injection the abort races ongoing churn, so the lock-release and placement-revert happen while other sequences hold adjacent locks — exactly where an orphaned-lock bug would hide. |
| **Why It Matters** | Dense historical bug surface: "Make nodetool abortbootstrap more robust", "Add nodetool command to abort failed cms initialize", "Avoid NPE during cms initialization abort". An abort that orphans a `LockedRange` or leaves a partial placement wedges all future movement over those ranges. |

**Open Questions:**

- Also exercise `cancelInProgressSequences` on a decommission/move, and `cancelReconfigureCms` on a CMS reconfiguration? `(hooks are wired in Harness; abortBootstrap on a stuck join is the first, most bug-dense case)`

### r-cms-reconfiguration-observed — CMS membership actually changes

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, a CMS reconfiguration is observed in flight. |
| **Invariant** | Workload-side `Assert.sometimes`, message `"a CMS reconfiguration was observed in progress"`, condition `describeCMS().IS_MIGRATING == true` on any node, or `reconfigureCMSStatus()` non-empty. `Sometimes` on a semantic state. |
| **Antithesis Angle** | Guards `c-cms-reconfiguration-quorum-overlap` and `c-commit-survives-cms-membership-change`, both of which are vacuous if reconfiguration never runs. |
| **Why It Matters** | CMS reconfiguration is both the most safety-critical and least-exercised TCM operation; it only happens during scaling events in production. |

**Open Questions:**

- None.

### r-snapshot-catchup-used — Nodes catch up via snapshot, not just entries

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, a node applies a `ForceSnapshot` entry to skip ahead rather than replaying individual entries. |
| **Invariant** | SUT-side `Assert.sometimes` in `LocalLog.processPendingInternal` on the `isSnapshot` branch, message `"a node caught up by applying a force snapshot"`, condition `isSnapshot && pendingEntry.epoch.isAfter(prev.epoch.nextEpoch())` (i.e. it genuinely jumped). `Sometimes` on the meaningful condition "the jump actually skipped entries," rather than `Reachable` on the branch, because entering the branch for a non-jumping snapshot proves nothing. |
| **Antithesis Angle** | Requires a node to fall far enough behind that the CMS serves a snapshot instead of entries — long partitions plus a high metadata change rate. This is the code path that CASSANDRA-21455 and the `693eab8776` revert both touched, and it is the one the `FORCE_SNAPSHOT` comparator priority exists to serve. |
| **Why It Matters** | The snapshot-jump path is the only code allowed to violate epoch consecutiveness. Untested, `a-no-gapped-metadata-published` only covers the easy case. |

**Open Questions:**

- None.

### r-commit-rejected — The validation path runs

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, a transformation is rejected by the CMS rather than committed. |
| **Invariant** | SUT-side `Assert.sometimes` in `AbstractLocalProcessor` where a `Reject` result is produced, message `"a transformation was rejected by the CMS"`, condition `!result.isSuccess()`. `Sometimes` because rejection is a meaningful semantic outcome. |
| **Antithesis Angle** | Rejections are linearized differently from successes (not persisted; confirmed by a read at the highest epoch), and `3e6a551dba` had to add catch-up-on-rejection. That asymmetric path needs to run under partition. |
| **Why It Matters** | Regression target for `3e6a551dba` "Catch up committing node on rejection" (CASSANDRA-19260) and `802ce7f8b2` "Always send TCM commit failures as Messaging failures." The workload deliberately submits conflicting operations (two joins on overlapping ranges) to force rejections. |

**Open Questions:**

- None.

### r-progress-barrier-relaxed — Consistency relaxation actually happens

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, a progress barrier relaxes below its default consistency level. |
| **Invariant** | SUT-side `Assert.sometimes` in `ProgressBarrier.await()` inside the relaxation loop, message `"a progress barrier relaxed below its default consistency level"`, condition `currentCL != DEFAULT_CL`. `Sometimes` on the state "we had to relax," which is exactly the risky configuration. |
| **Antithesis Angle** | Requires partitions that keep some affected-range owners unreachable long enough for relaxation but not so long that the sequence aborts. A narrow window that Antithesis is uniquely good at finding, and the necessary precondition for `b-progress-barrier-quorum-sound` to mean anything. |
| **Why It Matters** | Relaxation is where TCM knowingly trades safety margin for liveness. It is also the least likely path to be hit by conventional integration tests, which run on healthy networks. |

**Open Questions:**

- None.

### r-coordinator-behind-rejection — Epoch-divergence detection fires

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, a replica rejects a request with `CoordinatorBehindException`. |
| **Invariant** | **(R7)** Workload-side `Assert.sometimes`, message `"a replica rejected a request because the coordinator was behind"`, observed via `system_views.exceptions` (`db/virtual/ExceptionsTable.java`, registered in `SystemViewsKeyspace`) rather than a driver-visible error — the exception is thrown by a replica handling an internode message, so it is probably not surfaced to CQL clients. The observation mechanism must be confirmed before implementation: a `Sometimes` that cannot be observed is indistinguishable in a report from a state never reached, which reads as a pass. `Sometimes` on the semantic event. |
| **Antithesis Angle** | Needs a coordinator serving traffic at epoch N while a replica is at N+k with a *materially relevant* change between them. `TCM_implementation.md` is explicit that *immaterial* divergence does **not** throw — the replica catches up asynchronously instead — so **(R7)** the workload constructs materiality deliberately rather than hoping for it: it holds one single-host session per node, partitions a node from the CMS, runs DDL on the exact probe table that session queries, then queries through it. |
| **Why It Matters** | This is the request-path half of TCM's consistency story: metadata divergence is supposed to be *detected*, not just minimised. If it never fires, the harness is only testing the control plane and not its effect on reads and writes. |

**Open Questions:**

- Is `CoordinatorBehindException` surfaced to a CQL client as a distinguishable error, or only visible server-side? If only server-side, the check must read a metric or scan logs rather than catch a driver exception. `(partial: confirmed the exception class is referenced in TCM_implementation.md's Querying section as thrown by replicas; the client-visible mapping was not traced)`

## Category H — Harness self-checks

Properties about the *harness*, not the SUT. They exist because a harness can degrade into one
that always passes without anything in the report saying so.

### h-all-nodes-compared — Cross-node checks really do compare all nodes

| | |
|---|---|
| **Type** | Reachability |
| **Property** | At least once per run, a cross-node comparison includes every Cassandra node in the topology. |
| **Invariant** | Workload-side `Assert.sometimes`, message `"a cross-node comparison included every node"`, condition `evaluatedNodeCount == totalNodeCount`. Every cross-node checker additionally records its evaluated count. `Sometimes` on a meaningful condition: full participation is not required on every cycle (partitions legitimately prevent it) but must occur sometimes, or the cross-node properties were never really evaluated. |
| **Antithesis Angle** | Inverted — this property is *about* fault injection rather than driven by it. Faults are what cause nodes to be excluded, so this measures whether the faults were so pervasive that the checks became vacuous. |
| **Why It Matters** | At least six properties in this catalog are evaluated only over "nodes that answered." Excluding a partitioned node is correct; excluding a node whose process died is how the harness silently stops testing. Both look identical from the workload — a timeout. Without this property, a run in which two of five nodes were never compared would report the same green as a run that compared all five. It also catches a specific predicted failure: if `CMSOperations` does not register its MBean on nodes started with `join_ring=false`, both spares are permanently unobservable. |

**Open Questions:**

- Should a node whose container has exited be treated differently from one that is partitioned?
  Antithesis reports container exits, so a dead Cassandra process is separately visible — but the
  workload cannot tell the difference at check time, which is the whole point of this property.

## File-level Assumptions

- All properties are checkable through CQL virtual tables plus JMX, requiring no new
  Cassandra observability. Verified against `db/virtual/ClusterMetadataLogTable.java`,
  `db/virtual/ClusterMetadataDirectoryTable.java`, and `tcm/CMSOperationsMBean.java`.
- SUT-side assertions are confined to `src/java/org/apache/cassandra/tcm/`.
- The workload never calls `unsafeRevertClusterMetadata` or `unsafeLoadClusterMetadata`;
  those can manufacture states the protocol does not promise to survive.

## File-level Open Questions

- The gossip→TCM **upgrade** path is uncovered and is historically the densest bug area
  (`4318e74180`, `cdfce6b4ac`, `417bb21d2e`, `db94321d71`, `46b90364da`). It needs a
  mixed-mode deployment, so it is a second harness rather than an addition to this one.
- Accord's TCM coupling (consensus migration, `AccordMarkStale`,
  `ReconfigureAccordFastPath`) is deliberately excluded to keep failures attributable.
  Whether that exclusion is even achievable on `trunk` — where Accord is integrated by
  default — depends on whether Accord stays inert without Accord-enabled tables.
- Multi-datacenter placement and `EACH_QUORUM` barriers are not covered by the
  single-DC topology.
