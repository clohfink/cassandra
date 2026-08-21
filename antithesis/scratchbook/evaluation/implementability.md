---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-14
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Checked whether stated guarantees are observable at all from outside the process.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: Confirmed which operations the workload must be able to drive.
---

# Evaluation Lens 3 — Implementability

Can each property actually be checked, given the topology, the available observation surface,
and what the workload can drive?

Method: for every property, located the concrete observation call and the concrete action call.
Verified each against `CMSOperationsMBean`, `StorageServiceMBean`, and the registered virtual
tables in `SystemViewsKeyspace`.

## Findings

### F3.1 — `b-replication-factor-never-under` and `d-ring-fully-owned` are specified against the wrong observation surface

**Scope:** two properties, high severity. Both currently propose reconstructing placements from
`cluster_metadata_directory.tokens` plus the keyspace's replication settings. That means
**reimplementing Cassandra's placement algorithm inside the workload** — including its behaviour
during an in-flight movement, where write placements are deliberately expanded. The checker would
then be testing the workload's reimplementation against Cassandra's, and any disagreement would
be reported as a Cassandra defect.

This is worse than merely fragile: `b-replication-factor-never-under` is specifically about the
*intermediate* placements during a movement, which is exactly the part hardest to reimplement,
and `63c6261856` shows Cassandra's own settled-placement calculation was wrong once already.

**The fix exists and is direct.** `StorageServiceMBean` exposes the computed replica sets:

```java
public Map<List<String>, List<String>> getRangeToEndpointWithPortMap(String keyspace);
public Map<List<String>, List<String>> getPendingRangeToEndpointWithPortMap(String keyspace);
public List<String> describeRingWithPortJMX(String keyspace) throws IOException;
public Map<String, Float> effectiveOwnershipWithPort(String keyspace);
```

`getRangeToEndpointWithPortMap` gives range → replicas as Cassandra computed it, per node.
`getPendingRangeToEndpointWithPortMap` gives the pending (movement-expanded) set separately,
which is precisely the grow-before-shrink distinction `b-replication-factor-never-under` needs.
Range contiguity for `d-ring-fully-owned` comes from the same map's key set — no token
arithmetic in the workload at all.

**Suggested action:** respecify both properties against these two JMX calls. This makes them
simpler *and* stronger, and removes the entire "reimplement placement" risk.

### F3.2 — `b-locked-ranges-match-sequences` has no adequate observation surface

**Scope:** property-specific, high severity. `LockedRanges` is a `ClusterMetadata` component but
is **not** exposed by any virtual table or MBean method. Checked: `SystemViewsKeyspace`
registrations (`cluster_metadata_log`, `cluster_metadata_directory`, and non-TCM tables), and
every method on `CMSOperationsMBean` (`describeCMS`, `dumpLog`, `dumpDirectory`,
`reconfigureCMSStatus`, `dumpClusterMetadata`).

The property's own evidence file acknowledges this and proposes an indirect check — infer an
orphaned lock from prepares being rejected while no sequence exists. That inference is weak
(rejections have many causes) and cannot distinguish the two failure directions.

`dumpClusterMetadata()` returns a serialized dump of the whole `ClusterMetadata` to a file, which
*would* contain `lockedRanges` — but it writes server-side to a path the workload would then have
to read out of the container, which is a side channel that faults would disrupt.

**Suggested action:** move this property SUT-side. The natural site is the transformation that
removes a completed or cancelled sequence: assert the resulting metadata has no lock keyed to it.
That is a direct check of the bijection in the one place it can be violated, and it needs no new
observation surface. The workload-side version should be dropped rather than kept as a weak
proxy — a weak check on a P0 property is worse than an honest absence.

### F3.3 — `a-log-prefix-agreement`'s viability depends on an unresolved question, and has no
stated fallback

**Scope:** property-specific, high severity. The property's entire value rests on `dumpLog`
returning *this node's* view. Its investigation log correctly flags that if `dumpLog` proxies
`ClusterMetadataLogTable.log()` — which reads the distributed table at `QUORUM` — then calling it
on five nodes returns the same source five times and the cross-node comparison is vacuous.

This is the catalog's most consequential property (per `property-relationships.md`, it is the
root that several `d-*` properties are symptoms of), and it currently has a coin-flip
implementability status with no plan B written down.

**Suggested action:** resolve before implementation, and record the fallback now: a SUT-side
`Assert.always` in `LocalLog.processPendingInternal` comparing the entry about to be enacted
against the entry already persisted at that epoch in `LogStorage`, if any. That checks
immutability locally on every node rather than agreement across nodes — a weaker but still
valuable property, and one with no observation dependency at all.

### F3.4 — `a-metadata-identifier-unique` needs a workload action the topology does not yet define

**Scope:** property-specific, moderate. Corroborates F1.1 from the Antithesis Fit lens from the
implementability side: the property needs a node to re-enter `Startup`/`Discovery` during the
fault phase. `deployment-topology.md` lists `wipe-and-restart` among the control agent's
endpoints, but no workload action drives it and the property does not reference it.

**Suggested action:** add an explicit wipe-and-rejoin workload action, and reference it from the
property. Note the ordering constraint: a wiped node must be `unregister`ed
(`CMSOperationsMBean.unregisterLeftNodes`) or it will attempt to rejoin with a `NodeId` whose
tokens are already assigned — which is a legitimate rejection, not the discovery path we want.

### F3.5 — `r-coordinator-behind-rejection` requires driver configuration the topology has not specified

**Scope:** property-specific, moderate. The property needs to coordinate a query *through a
specific node* that the workload knows is lagging. A default driver configuration load-balances
and would route around it. The property's evidence file states this requirement but
`deployment-topology.md` does not carry it as a constraint on the workload container.

**Suggested action:** the workload must hold, in addition to a normal load-balanced session, one
single-host session per Cassandra node. Record this in the topology as a workload requirement.

## Verified as implementable

Each of these was checked against a specific, existing call:

| Property | Observation | Action |
|---|---|---|
| `a-epoch-monotonic-per-node` | `describeCMS().EPOCH` + SUT-side at `LocalLog:555` | control agent `restart` |
| `a-no-gapped-metadata-published` | SUT-side, `LocalLog:542/544` | none needed |
| `c-cms-membership-never-empty` | `describeCMS().MEMBERS` + `.EPOCH` | `reconfigureCMS(rf)` |
| `c-cms-reconfiguration-quorum-overlap` | SUT-side in `ReconfigureCMS.advance` | `reconfigureCMS(rf)` |
| `c-commit-survives-cms-membership-change` | `dumpLog` / `cluster_metadata_log` | DDL with unique table names |
| `d-schema-agreement-at-same-epoch` | `getSchemaVersion()` + `describeCMS().EPOCH` | DDL |
| `d-peers-table-matches-directory` | `SELECT ... system.peers_v2` + `cluster_metadata_directory` | replace via control agent |
| `e-cluster-converges-after-faults` | `describeCMS()` `EPOCH` + `LOCAL_PENDING` | `eventually_` command |
| `e-cms-accepts-commits-after-recovery` | DDL round trip + per-node epoch visibility | `setCommitsPaused(false)` first |
| `r-concurrent-multistep-operations` | `cluster_metadata_directory.multi_step_operation` | `joinRing()`, `decommission(force)`, `move(token)` |
| `r-cms-reconfiguration-observed` | `describeCMS().IS_MIGRATING`, `reconfigureCMSStatus()` | `reconfigureCMS(rf)` |
| `r-snapshot-catchup-used` | SUT-side in `processPendingInternal` | `snapshotClusterMetadata()` + partitions |
| `r-commit-rejected` | SUT-side in `AbstractLocalProcessor` | overlapping concurrent operations |
| `r-progress-barrier-relaxed` | SUT-side in `ProgressBarrier.await` | partitions |
| `b-progress-barrier-quorum-sound` | SUT-side in `ProgressBarrier.await(cl, metadata)` | partitions |
| `b-sequence-resumable-after-crash` | `multi_step_operation` + `MultiStepOperation.status()` | control agent kill/restart |
| `b-no-overlapping-locked-ranges` | SUT-side in `LockedRanges.lock` | concurrent overlapping operations |
| `a-metadata-identifier-unique` | `describeCMS().CMS_ID` | see F3.4 |

Confirmed present on `StorageServiceMBean`: `joinRing()`, `decommission(boolean force)`,
`move(String newToken)`, `getOperationMode()`, `isBootstrapMode()`, `getSchemaVersion()`.
Confirmed on `CMSOperationsMBean`: all mutators referenced above.

## Topology adequacy

- Five nodes in five containers: partitions can isolate any subset. Adequate for every network
  fault a property needs.
- Node termination being off is correctly compensated by the control agent, and replacement
  (which needs a JVM flag) is only possible because of it. Without the agent, three properties
  and all replacement coverage would be unimplementable — the agent is load-bearing, not
  convenience.
- One workload container holding cross-node state is required by
  `a-epoch-monotonic-per-node` (per-node high-water marks) and
  `c-commit-survives-cms-membership-change` (the tag ledger). Confirmed correct.

## Uncertainties

- Whether `-Dcassandra.join_ring=false` on `trunk` still registers the node and follows the log.
  Two properties' preconditions depend on it. Verifiable in the first local compose run; already
  an open question in `deployment-topology.md`.
- Whether the `CMSOperations` MBean is registered on a node started with `join_ring=false`.
  `CMSOperations` registers itself at `org.apache.cassandra.tcm:type=CMSOperations`, but if
  registration happens after ring join, spares would be unobservable and several cross-node
  checks would silently drop two of five nodes.
- Whether `getPendingRangeToEndpointWithPortMap` is still populated under TCM, or is a
  gossip-era API left in place for compatibility. TCM replaced `PendingRanges` with
  placement-based expansion, so this method may return empty. If so, F3.1's fix needs
  `getRangeToEndpointWithPortMap` alone plus the write/read placement distinction from
  `describeRingWithPortJMX`. Worth checking early since F3.1 depends on it.
