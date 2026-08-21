# a-metadata-serialization-round-trips

## What led to this property

Cluster metadata does not live only in memory. It is serialized on every commit response, every
`FetchCMSLog`/`FetchPeerLog` reply, every snapshot written to `system_cluster_metadata`, and every
persisted log entry. A peer or a restarting node reconstructs its entire view of the ring, schema,
placements and in-flight operations by **deserializing** that form. If the serializer and
deserializer disagree about even one field, the reconstructed metadata is silently wrong — and the
symptom shows up far away, epochs later, as "a node can't catch up" or "two nodes disagree about an
epoch," which is expensive to trace back to a serializer bug.

Mining `git log -- src/java/org/apache/cassandra/tcm` shows this is not hypothetical and not
historical — it is an active, recurring bug class:

- `1913eab974` — "Fix deserialization of column masks in cluster metadata"
- `2bc24da841` — "Allow empty placements when deserializing cluster metadata"
- `9af2b2cdf8` — "Improve performance deserializing cluster metadata"
- `415eaffb9c` — "Reduce heap pressure when initializing CMS" (touches the same path)

None of the existing 30 properties exercised serialization at all — the harness compared *live*
metadata across nodes but never round-tripped the *encoded* form.

## What it guards

The reconstruction contract for every consumer of serialized metadata: peers catching up, nodes
replaying at startup, snapshot readers. It is upstream of `a-log-prefix-agreement` and
`r-snapshot-catchup-used` — both assume that what a node deserializes equals what the author
serialized. This property tests that assumption directly instead of relying on a downstream
divergence to reveal its failure.

## Code involved

- `tcm/ClusterMetadata.java` — `Serializer.serialize` / `deserialize` / `serializedSize`, and
  `equals` (the round-trip comparison).
- `tcm/serialization/Version.java` — `minCommonSerializationVersion()`, the version the cluster
  actually encodes with (so the check tracks whatever the live cluster negotiates).
- `tcm/log/LocalLog.java` — the callsite, in `processPendingInternal` inside the
  `committed.compareAndSet(prev, next)` block, so it runs once per published epoch.
- `io/util/DataOutputBuffer` / `DataInputBuffer` — the in-memory round-trip.

## Defining the condition took two Antithesis runs — BOTH naive checks false-positive

`ClusterMetadata` round-tripping has **two independent benign asymmetries**, each of which sinks a
naive check:

1. **`equals()` is stricter than the wire form.** Run `efcbee84` (equals-based check) reported 72
   counterexamples, all `error:null` on `PREPARE_COMPLEX_CMS_RECONFIGURATION` /
   `ADVANCE_CMS_RECONFIGURATION`: byte-identical round-trip yet `equals()` false — a transient/identity
   field on the CMS-reconfiguration in-progress sequence that is not serialized. Not data loss.
2. **Byte-stability is stricter than the data.** Run `c3c05902` (byte-stability check) then reported
   127 counterexamples on `PREPARE_JOIN` / `START_JOIN` / `MID_JOIN`: re-serialized bytes differ even
   though serialization is deterministic (`selfStable=true`) — a map reconstructed in a different
   iteration order on deserialize. `equals()` (order-independent) is true. Not data loss.

Measured locally (4-node join + setup-cms), each kind trips **exactly one** signal:

| kind | equals() | bytes identical |
|------|----------|-----------------|
| PREPARE_JOIN / START_JOIN / MID_JOIN | true  | false |
| ADVANCE_CMS_RECONFIGURATION / PREPARE_COMPLEX_CMS_RECONFIGURATION | false | true |

A genuine corruption (a dropped/mangled field) shows in **both** — the object is unequal *and*
re-serializes to different bytes. So the condition is:

> **fail iff deserialize throws, or (`!equals()` AND re-serialized bytes differ).**

i.e. `faithful = equalsOk || bytesOk`. Each benign asymmetry trips one signal and is tolerated; the
historical bug shapes (deserialize throwing — `2bc24da841` empty placements; dropping a field —
`1913eab974` column masks) trip both (or throw) and are caught. Verified locally: 0 "differs in BOTH"
across all join and CMS-reconfiguration transformations.

(Two minor code observations, neither a correctness bug: `ClusterMetadata.equals()` is inconsistent
with the serialized form for CMS-reconfiguration sequences, and join-sequence serialization is
order-unstable across a round-trip. Worth maintainer notes; neither loses data on the wire.)

## Why `Always` and not `AlwaysOrUnreachable`

The publication path runs on every enacted entry, so the check is not optional — if metadata is ever
published, it is reachable. A round-trip that throws or returns a non-equal object is a defect in
every execution, never an acceptable outcome.

## Why it is gated

Serializing the full `ClusterMetadata` on every committed epoch is cheap for a 10-node simulated
cluster but is pure overhead on a production node with large schema/placement state. The check is
therefore gated on `-Dcassandra.antithesis.serialization_check=true`, set only by the Antithesis node
image (`antithesis/docker/entrypoint-cassandra.sh`). Unset in production, the static
`AntithesisDetails.SERIALIZATION_CHECK` is `false` and the block is skipped entirely.

## How the workload makes it meaningful

The workload never needs to do anything special for this property — it fires on every epoch. What
makes it *valuable* is that fault injection and the other drivers push metadata through unusual
shapes right before it is round-tripped: placements mid-movement with locked ranges, two MSOs in
flight, CMS mid-reconfiguration, schema carrying column masks and UDTs. Those are exactly the states
where a serializer edge case hides, and they are published (and therefore round-tripped) as they
occur.

## Open questions

- Extend to assert `serializedSize(next) == bytesActuallyWritten`? A size/serialize mismatch is a
  separate common serializer bug (over-allocation or truncation) that the equality check alone would
  not always catch. Candidate refinement, not yet implemented.
