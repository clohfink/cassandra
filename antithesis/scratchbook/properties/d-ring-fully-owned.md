# d-ring-fully-owned

## What led to this property

CEP-21 downgrades range merging to an optimisation: post-decommission range merging "is
essentially an optimisation and isn't strictly necessary." Code that is "not strictly
necessary" gets less scrutiny, and range-boundary arithmetic is where off-by-ones live.
Combined with `8404d2fd5c` ("Improve performance when getting `writePlacementAllSettled`
from ClusterMetadata in large cluster with many range movements") and `7fb21c323a`
("Optimize DataPlacement lookup by ReplicationParams") — two performance rewrites of the
placement lookup path — there is a plausible route to a boundary regression.

Also `2bc24da841` "Allow empty placements when deserializing cluster metadata" and
`ec7794f20f` "Avoid NPE when meta keyspace placements are empty before CMS is initialized":
empty placements are a real state the code has had to accommodate, which means "placements
cover the ring" is not universally true and the property needs a precise scope.

## Code involved

- `tcm/ownership/TokenMap.java` — token → node mapping; has `assert`/`Invariants` usage per
  the TCM assert scan, so it already carries structural invariants worth reading.
- `tcm/ownership/ReplicaGroups.java`, `tcm/ownership/DataPlacements.java`,
  `tcm/ownership/PrimaryRangeComparator.java`.
- `tcm/ClusterMetadata.java:113,114` — `tokenMap`, `placements`.
- `db/virtual/ClusterMetadataDirectoryTable.java` — `tokens` column
  (`ListType.getInstance(UTF8Type.instance, false)`), the observation source.
- `dht/Range`, `dht/Token` — wrap-around semantics.

## What goes wrong if violated

- **Gap** (a range no replica set claims): reads over those tokens return nothing; writes are
  either rejected or silently accepted by a coordinator that then finds no replicas. Data in
  that range becomes unreachable while remaining on disk.
- **Ambiguity** (two disjoint replica sets claiming one range): both sets accept writes for
  the same keys. Two independent histories for those partitions, reconciled by nothing —
  last-write-wins across sets that never exchange data.

Both are worse than under-replication because they are not detectable by repair.

## Exclusions that must be right

Three, and each would otherwise generate false failures:

1. **The metadata keyspace.** CEP-21: "all CMS nodes own an entire range from MIN to MAX
   token," and `ClusterMetadataLogTable` uses `MetaStrategy.partitioner` — a different
   partitioner from the user keyspaces'. Ranges from `MetaStrategy` are not comparable with
   user-keyspace ranges and must not be merged into the same contiguity check.
2. **Pre-initialisation and empty placements.** Guard on `epoch >= FIRST`, same as
   `c-cms-membership-never-empty`.
3. **Transient replication**, if enabled. A transient replica is a real replica for
   contiguity purposes but not for RF counting, so this property and
   `b-replication-factor-never-under` need different treatment of it. The harness does not
   enable transient replication, which sidesteps this — recorded because enabling it later
   would silently change what both properties mean.

## Implementation notes

- Reconstruct ranges from the sorted token list per keyspace-replication-setting, then check
  that consecutive ranges abut and that the sequence wraps exactly once through the
  partitioner's minimum token. Checking "no gaps" without checking "wraps exactly once"
  misses the case where the ring is covered twice.
- Per-node, per-epoch: each node's own view must be internally consistent. A node behind by
  several epochs still has a fully-owned ring — just an older one. This formulation makes lag
  irrelevant, which is the same trick used in `b-replication-factor-never-under`.
- Vnodes multiply the token count per node; with the harness's small `num_tokens` the
  reconstruction stays cheap enough to run on every check cycle.

## Investigation Log

#### Do transient replicas or the `MetaStrategy` metadata keyspace need excluding?

- Examined: `db/virtual/ClusterMetadataLogTable.java` — `.partitioner(MetaStrategy.partitioner)`
  on the table builder, confirming a distinct partitioner for metadata;
  `db/virtual/ClusterMetadataDirectoryTable.java` — `.partitioner(new LocalPartitioner(LongType.instance))`,
  a *third* partitioner, for the directory view itself; CEP-21's statement that CMS nodes own
  MIN→MAX; `ClusterMetadata.java`'s `partitioner` field, documented in-line as "Set during
  (initial) construction and not modifiable via Transformer".
- Found: the metadata keyspace definitively needs excluding — it uses `MetaStrategy` with its
  own partitioner and full-range ownership, so it has no meaningful range structure to check.
  The user-keyspace partitioner is fixed at cluster construction and immutable, so a single
  contiguity check per replication setting is well-defined.
- Not found: whether transient replication is representable in `DataPlacements` in a way that
  the directory-derived reconstruction would even see. `ownership/ReplicaGroups.java` would
  answer it.
- Conclusion: tagged `(partial)`. Sidestepped by not enabling transient replication in the
  harness, which is the right call for a first harness — it is an orthogonal feature and
  mixing it in would make any failure ambiguous between the two subsystems. Noted as a
  prerequisite to revisit if transient replication is ever added to the topology.
