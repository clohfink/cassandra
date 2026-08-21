# r-snapshot-catchup-used

## What led to this property

The snapshot-jump path is the **only** code in TCM permitted to violate epoch
consecutiveness. Everything else must satisfy `isDirectlyAfter`. That makes it the single
highest-risk branch in `LocalLog`, and `a-no-gapped-metadata-published` only tests the easy
case unless this path actually runs.

Evidence it is fragile:

- `44ee9d6167` "Unable to catch up TCM Log from peer with gaps in log sequence" (CASSANDRA-21455).
- `693eab8776` "Revert changes to serving FetchCMSLog/FetchPeerLog requests & remove
  ReconstructLogState" — an attempt at this code was backed out.
- `c16297f862` "Add an ability to reconstruct arbitrary epoch state from the log to TCM" and
  `9af2b2cdf8` "Improve performance deserializing cluster metadata" — active churn.

## Code involved

`tcm/log/LocalLog.java`. The pieces that only matter for snapshots:

- The `pending` comparator (line 242), which deliberately orders `FORCE_SNAPSHOT` first:

  > we are using a custom comparator for pending entries, which ensures that FORCE_SNAPSHOT
  > entries are going to be prioritised over other entry kinds. After application of the
  > snapshot entry, any entry with epoch lower than the one that snapshot has enacted, are
  > simply going to be dropped.

- `boolean isSnapshot = kind == Transformation.Kind.FORCE_SNAPSHOT;` (line 512) and its use in
  the skip-permitting guard at 518–519 and the post-condition assert at 544.
- `snapshotListener()` (line 955).

Also `tcm/transformations/ForceSnapshot.java`, `TriggerSnapshot.java`,
`tcm/MetadataSnapshots.java`, and `CMSOperationsMBean.snapshotClusterMetadata()`.

From `TransactionalClusterMetadata.md`, the mechanism:

> To do this, it constructs a synthetic log entry containing a `ForceSnapshot` transformation
> which it inserts at the head of its local buffer of pending log entries.

## Why the condition is "genuinely jumped" and not "branch entered"

`Reachable` on the `isSnapshot` branch would fire on a snapshot that happens to arrive at
exactly the next epoch — which is a no-op jump and proves nothing about the gap-skipping logic.
The condition is therefore `isSnapshot && pendingEntry.epoch.isAfter(prev.epoch.nextEpoch())`:
the snapshot skipped at least one epoch. That is the semantic state that makes
`a-no-gapped-metadata-published` non-vacuous, which is the whole reason this property exists.

This is the distinction the SDK docs draw between `Sometimes(cond)` with a meaningful
condition and `Reachable` on a line, and it is the difference between "the snapshot code ran"
and "the snapshot code did the dangerous thing."

## The reachability problem

Snapshots are served when a node has fallen far enough behind that entry-by-entry catch-up is
not worth it. In a short test run with a low metadata change rate, no node ever falls that far
behind — so this property is the one most likely to *not* fire, and the most informative when
it does not.

Three levers the harness pulls, in order of directness:

1. **`snapshotClusterMetadata()` on demand.** The workload calls it periodically via JMX, so
   snapshots exist in the log to be served.
2. **Long partitions plus high metadata churn.** A node partitioned from the CMS while DDL
   proceeds accumulates a large gap. Antithesis network faults do this naturally; the
   workload's job is to keep the change rate high enough that the gap gets large.
3. **Node restarts after a partition.** A restarted node replays from persisted state and then
   must catch up from wherever it left off — the path CASSANDRA-19384 broke and the one where a
   snapshot is most likely to be the efficient answer.

If it still does not fire, the honest reading is that the run did not test gap-skipping, and
the response is a longer run or a higher churn rate — not lowering the bar.

## Implementation notes

- SUT-side, in `processPendingInternal`, inside the successful-enactment block where `isSnapshot`
  and both epochs are in scope. A workload-side version is not possible: nothing external can
  observe that a particular epoch transition was a snapshot jump rather than a sequence of
  entries.
- The `Details` payload should carry `prev.epoch`, `pendingEntry.epoch`, and the size of the
  jump. The jump size is the number worth watching over time: consistently small jumps mean the
  harness is not producing real lag.
- This assertion sits three lines from the two `a-no-gapped-metadata-published` assertions, so
  all three should be added in one edit to `LocalLog` with distinct messages.
