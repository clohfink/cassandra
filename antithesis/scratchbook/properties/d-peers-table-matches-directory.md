# d-peers-table-matches-directory

## What led to this property

The strongest possible evidence: Cassandra ships a *repair tool* for this exact
inconsistency. Commit `c484fc511a`, "Add tooling to repair system peers tables if
inconsistent with cluster metadata." A tool exists because the drift happens in the field.

The specific fixes:

- `32755cabfa` "Correctly update peers tables following replacement" (CASSANDRA-19782)
- `38512a469c` "Fix peers v2 system table behaviour when 2 nodes swap their IP Addresses"
- `51ef21b6bc` "Fix gossip status after replacement"
- `cdfce6b4ac` "Allow nodes to change IP address while upgrading to TCM"
- `46b90364da` "Change IP address of the CMS node during transition"

Replacement and address change dominate. That is the workload shape this property wants.

## Code involved

- `tcm/membership/Directory.java` — the authoritative node registry inside `ClusterMetadata`
  (identity, `NodeState`, location, addressing).
- `tcm/listeners/` — the listeners that write derived state, including the peers tables.
- `db/SystemKeyspace` — `system.peers_v2`, `system.local`.
- `db/virtual/ClusterMetadataDirectoryTable.java` — the comparison source. Its columns are
  exactly what is needed: `node_id`, `host_id`, `state`, `rack`, `dc`, `broadcast_address`,
  `broadcast_port`, `local_address`, `local_port`, `native_address`, `native_port`, `tokens`,
  `multi_step_operation`. Crucially it reads `ClusterMetadata.current()` **locally**:

  ```java
  public static Map<Long, Map<String, Object>> directory(boolean tokens)
  {
      ClusterMetadata metadata = ClusterMetadata.current();
      Directory directory = metadata.directory;
      ...
  ```

  So it is genuinely this node's own belief, which is what makes the comparison meaningful.
- `tcm/CMSOperationsMBean.java` — `dumpDirectory(boolean includeTokens)`, the JMX equivalent.

## What goes wrong if violated

`system.peers_v2` is what drivers read to build their token map. Drift means token-aware
routing sends requests to nodes that do not own the key. Cassandra will still answer
correctly — the coordinator forwards — so the failure is invisible in correctness terms and
shows up only as latency and cross-node traffic. That invisibility is why it survived to need
a repair tool.

The more serious variant: a peer whose `host_id` is stale after a replacement. Host ID is
used for hint delivery and for distinguishing "same address, different node." A stale one can
route hints to a node that no longer owns the data.

## The scoping problem, which is the whole difficulty

`Directory` tracks `NodeState`, and only some states are supposed to have `peers_v2` rows.
A node that is `REGISTERED` (has a `NodeId`, no tokens yet), `BOOTSTRAPPING`, or `LEFT` is
in a transitional or terminal state. Getting the exclusion set wrong makes the property
either noisy (fires on every join) or vacuous (excludes everything interesting).

The mitigation used here is to scope by *sequence activity* rather than by enumerating
states: exclude any node with a non-empty `multi_step_operation`, and require the observation
to be stable across two consecutive samples. That is robust to not knowing the exact state
machine, at the cost of not checking mid-movement — which is acceptable because the
historical bugs all manifested as drift that *persisted after* the movement completed, not
during it.

## Implementation notes

- Compare on `host_id` and `tokens` primarily; addresses secondarily. Address comparison
  must normalise the address/port split (`broadcast_address` + `broadcast_port` in the
  directory vs. `peers_v2.peer` + `peer_port`).
- Self must be excluded from `peers_v2` and checked against `system.local` instead — a node
  does not list itself as a peer.
- Run this on every node, not just one. The whole point is that derived state is *local*;
  checking one node would miss drift on the other four.

## Local run finding (2026-08-18) — checker false positive, fixed

The first live local run reported this property failing 9 times, all of the form
`in_directory_but_not_peers_v2: ["172.19.0.2"]` where `172.19.0.2` was the node's *own* address.
A node never lists itself in `system.peers_v2`, so its own directory entry must be excluded — and
the exclusion was matching by address (`n.host`, a container hostname like `cassandra-1`) against a
directory `broadcast_address` stored as a bare IP (`172.19.0.2`). Hostname ≠ IP, so self was never
excluded.

Fixed by identifying self via `SELECT host_id FROM system.local` and excluding the directory entry
with that `host_id` — the one identity both tables agree on. This is a checker bug, not a TCM bug:
the peers table and directory agreed on every real node.

## Investigation Log

#### Which `NodeState` values legitimately have no `peers_v2` row?

- Examined: `db/virtual/ClusterMetadataDirectoryTable.java` in full (columns, and the
  `directory(boolean)` method reading `ClusterMetadata.current()` locally);
  `tcm/membership/` package listing showing `Directory` and `NodeState`;
  `CMSOperationsMBean.dumpDirectory(boolean)` and `unregisterLeftNodes(List<String>)` — the
  latter proving `LEFT` nodes persist in the directory until explicitly unregistered, and
  `nodetool cms unregister` exists for "Unregister nodes in LEFT state".
- Found: `LEFT` nodes definitively remain in the directory after leaving, so the directory is
  a superset of `peers_v2` and the comparison cannot be a plain equality. `REGISTERED`
  exists as a distinct pre-join state (`tcm/RegistrationStatus.java`), and
  `TCM_implementation.md` confirms registration happens before joining the ring — so a
  registered-not-joined node has a `NodeId` and no tokens.
- Not found: the `peers_v2` write path's own state filter. Locating it means finding which
  listener in `tcm/listeners/` owns the peers tables, which was beyond discovery scope.
- Conclusion: tagged `(partial)`. Resolved *around* rather than resolved: the
  sequence-activity scoping plus two-sample stability avoids needing the exact state set, and
  the harness's `cms unregister` calls give a deterministic way to remove `LEFT` nodes from
  the comparison entirely. Worth revisiting if the property proves noisy in practice, since
  the precise filter would allow checking during movements too.
