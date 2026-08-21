# a-log-prefix-agreement

## What led to this property

CEP-21 states it as a flat fact: "Both epochs and the change log itself are immutable and
once an event is assigned a particular order in the log, this cannot be modified." Every
other TCM guarantee is a corollary — if two nodes can hold different entries at the same
epoch, then `ClusterMetadata` is no longer a function of the log, and schema agreement,
placement agreement, and quorum-overlap reasoning all collapse simultaneously.

The reason to test it despite it sounding tautological is that the *log content* is
replicated three different ways, and only one of them is consensus-protected:

1. Paxos CAS into `system_cluster_metadata.distributed_metadata_log` (protected).
2. Best-effort broadcast to all peers via `Replicator` — TCM_implementation.md is explicit
   that this "does not need to be reliable and has no retries."
3. Pull-based catch-up: `TCM_FETCH_PEER_LOG_REQ` from an arbitrary peer, falling back to
   `TCM_FETCH_CMS_LOG_REQ`.

Path 3 is the risk. CEP-21 justifies serving log entries from *any* peer on the grounds
that "the log is immutable and totally ordered, this request can be made to any peer as
the results must be consistent, but not exhaustive." That justification is circular with
respect to this property: peer-serving is safe *because* the log agrees, and the log
agrees partly *because* peer-serving is correct.

## Code involved

- `tcm/log/LocalLog.java` — `append(LogState)`, `maybeAppend(Entry)`, `getLocalEntries(Epoch)`.
  `getLocalEntries` is what a node hands to a peer that asks it to catch up.
- `tcm/FetchPeerLog.java`, `tcm/FetchCMSLog.java`, `tcm/PeerLogFetcher.java` — the fetch verbs.
- `tcm/log/Replicator` (referenced from the CMS commit path) — the unreliable broadcast.
- `tcm/PaxosBackedProcessor.java` — the consensus-protected append.
- `tcm/CMSOperations.java` — `dumpLog(startEpoch, endEpoch)`, the observation primitive.

## What goes wrong if violated

Two nodes compute divergent `ClusterMetadata` at the same epoch. Because the epoch is the
identity used in every internode message for divergence detection, the divergence becomes
*undetectable*: two nodes both at epoch 500 will not attempt to catch each other up, since
neither is behind. Coordinators and replicas would use different replica sets while both
believing they agree. This is the one TCM failure mode with no self-healing path.

## Expensive-to-rediscover details

- `dumpLog` returns `Map<Long, Map<String, String>>` keyed by epoch, with the same
  `KIND`/`TRANSFORMATION`/`ENTRY_ID`/`ENTRY_TIME` shape as `cluster_metadata_log`.
  Compare `KIND` and `ENTRY_ID`, not `TRANSFORMATION`: the transformation column is
  `transformation.toString()`, and comparing rendered strings across nodes risks tripping
  on formatting differences (e.g. a `toString` that embeds a set whose iteration order is
  not stable) rather than on real disagreement.
- The check must intersect epoch key sets first. A node that is legitimately behind has a
  *shorter* log, which is expected and must not be reported.
- `system_views.cluster_metadata_log` is **not** a substitute: `ClusterMetadataLogTable.log()`
  issues a `SELECT ... AT QUORUM` against the metadata table, so on every node it returns
  the CMS's view rather than that node's own. It cannot detect local divergence at all.

## Local run finding (2026-08-18) — checker false positive, fixed

The first live local run reported this property failing on every evaluation. The directory contents
were in fact identical across nodes; the *rendering* differed. A node renders its own directory
entry's addresses as `hostname/ip` (`cassandra-1/172.19.0.2`) because the local `InetAddress`
carries the resolved hostname, while every peer renders that same node as `/ip`. That is an
`InetAddress.toString()` display artifact, not a `ClusterMetadata` difference — the stored
`InetAddressAndPort` is identical.

This is precisely the formatting fragility this file's evidence already warned about ("comparing
rendered strings across nodes risks tripping on formatting differences ... rather than on real
disagreement") — the lesson was applied to the log KIND/ENTRY_ID comparison but then reintroduced
when the property was re-implemented against the directory rendering.

Fixed by comparing through a `canonicalDirectory()` helper that reduces every address field to its
bare IP via `normalizeAddress()` before comparison. A genuine address divergence (different IP)
still shows; only the hostname-prefix noise is dropped. After the fix the property held across the
re-run. A checker bug, not a TCM bug.

## Investigation Log

#### Does `dumpLog` read local in-memory log state, or the distributed metadata table?

**RESOLVED (2026-08-14), and the answer was the bad one.** `CMSOperations.dumpLog` is three lines:

```java
public Map<Long, Map<String, String>> dumpLog(long startEpoch, long endEpoch)
{
    Map<Long, Map<String, Object>> log = ClusterMetadataLogTable.log(startEpoch, endEpoch);
    return convertToStringValues(log);
}
```

It delegates to `ClusterMetadataLogTable.log()`, which issues
`SELECT ... FROM system_cluster_metadata.distributed_metadata_log` at
`ConsistencyLevel.QUORUM`. So calling `dumpLog` on five nodes returns the CMS's single authoritative
view five times. **The cross-node comparison as originally specified could never fail.** The risk
flagged in the original investigation was real.

**What was done instead.** Not the SUT-side fallback drafted in refinement R3 (comparing against
`LogStorage` on every enactment would mean a storage read on the metadata publication path). Instead
the property is checked through its *consequence*, using state that genuinely is per-node:

`CMSOperations.dumpDirectory` is backed by `ClusterMetadata.current()` on the node being asked —
confirmed in `ClusterMetadataDirectoryTable.directory()`, which opens with
`ClusterMetadata metadata = ClusterMetadata.current();`. That is the node's own belief, not a
distributed read. Since `ClusterMetadata` is a deterministic function of the log prefix, two nodes
reporting the same epoch must have identical directories — so a directory disagreement *within one
epoch* is a log disagreement, observed one step downstream.

The checker groups nodes by reported epoch, re-reads the epoch after sampling the directory to
discard samples that straddled an enactment, and renders through a `TreeMap` so that `HashMap`
iteration order cannot produce a spurious mismatch.

**What this costs.** The check now covers the directory component (identity, state, tokens, in-flight
sequences); schema is covered by `d-schema-agreement-at-same-epoch`. Components not surfaced by
either — `lockedRanges`, `placements` beyond what tokens imply, `consensusMigrationState`,
`extensions` — are not compared. A log divergence confined to one of those would be missed.

**How to strengthen it.** A read-only JMX method exposing `LocalLog`'s *own* view of its entries
(`LocalLog.getLocalEntries(Epoch)` already exists and returns a `LogState`) would restore the direct
entry-by-entry comparison and make this the strongest property in the catalog rather than an
indirect one. That is a small addition to `CMSOperationsMBean` and is the single highest-value
observability change this harness would ask for.

Original investigation, retained as audit trail:

- Examined: `tcm/CMSOperationsMBean.java` (signature `Map<Long, Map<String,String>> dumpLog(long, long)`),
  `tcm/CMSOperations.java` (`describeCMS` implementation read in full for comparison),
  `db/virtual/ClusterMetadataLogTable.java` (`log()` reads the metadata table at
  `ConsistencyLevel.QUORUM`).
- Found: the virtual table definitively reads the distributed table at QUORUM. The two
  share a row shape, which suggests common ancestry.
- Not found: `dumpLog`'s own body. If it delegates to `ClusterMetadataLogTable.log()`, the
  cross-node comparison is reading one source N times and the property becomes vacuous.
- Conclusion: tagged `(partial)`. Resolution is cheap and mandatory before implementing
  the checker: read `CMSOperations.dumpLog`. If it proxies the table, substitute
  `LogStorage.getPersistedLogState()` exposed through a new read-only JMX method, or
  compare via a SUT-side assertion in `LocalLog` instead of a workload-side one.
