# b-locked-ranges-match-sequences

## What led to this property

Commit `80971709b9`, "Properly set lastModifiedEpoch on multistep operations"
(CASSANDRA-19538). `LockedRanges` keys are epoch-derived (`LockedRanges.keyFor(Epoch)`) and
both `LockedRanges` and `MultiStepOperation` carry a "last modified" epoch
(`LockedRanges.withLastModified(Epoch)`, `MultiStepOperation.latestModification`). A bug in
maintaining that epoch is directly a bug in the correspondence between a lock and its owner,
because the epoch *is* the link.

Related: `60fe2dc61d` "Fix version check in InProgressSequences serialization" — the
sequence set has had serialization bugs, and it is serialized into every `ClusterMetadata`
snapshot and log entry.

## Code involved

- `tcm/sequences/LockedRanges.java` — `locked` map keyed by `Key(Epoch)`; `unlock(Key)`.
- `tcm/sequences/InProgressSequences.java` — the sequence registry inside `ClusterMetadata`.
- `tcm/MultiStepOperation.java` — `latestModification`, `idx`, `sequenceKey()`,
  `advance(CONTEXT)`, and:

  ```java
  public ClusterMetadata.Transformer cancel(ClusterMetadata metadata)  // line 262
  ```
- `tcm/transformations/CancelInProgressSequence.java` — the operator-initiated cancel.
- `tcm/sequences/CancelCMSReconfiguration.java` — the CMS-specific cancel.
- `tcm/CMSOperationsMBean.java` — `cancelInProgressSequences(String sequenceOwner, String expectedSequenceKind)`.

Observation: `cluster_metadata_directory.multi_step_operation` is a
`map<text, text>` column, populated per node from `ClusterMetadata.current()`. That is the
workload's window into which sequences exist and who owns them.

## What goes wrong if violated

Asymmetric failure, both directions bad:

- **Orphaned lock** (lock without sequence): every future `PrepareJoin`/`PrepareLeave`/
  `PrepareMove`/`PrepareReplace` touching that range is rejected forever. The cluster is
  permanently unable to change topology in that range. `unlock(Key)` is keyed by the
  originating epoch, so once the sequence is gone there is no supported way to name the key
  — recovery means `unsafeRevertClusterMetadata` or `unsafeLoadClusterMetadata`, the
  escape hatch CEP-21 flags as "a high degree of risk."
- **Unlocked sequence** (sequence without lock): the admission gate is open. A second,
  overlapping operation can now be admitted, which is `b-no-overlapping-locked-ranges`
  failing by a different route.

The first is the more likely and the more damaging operationally: a cluster that looks
completely healthy but silently refuses to scale.

## Why the crash-between-prepare-and-first-step window is the target

`PrepareJoin` locks ranges and registers the sequence in one transformation, so those are
atomic. The exposure is at the *end*: the sequence completes, and unlocking plus sequence
removal must also be atomic. TCM_implementation.md: "Upon executing all steps in the
progress sequence, ranges are unlocked, and sequence itself is removed from
`ClusterMetadata`." Stated as one sentence; the question is whether it is one transformation.

Cancellation is the second exposure, and it is the one an operator actually triggers. The
workload therefore calls `cancelInProgressSequences` mid-sequence deliberately, rather than
only waiting for natural completion.

## Implementation notes

- Read `multi_step_operation` from every reachable node and compare against that node's own
  lock state. `LockedRanges` is not directly exposed by a virtual table, which is a real
  gap: the check can currently detect "sequence with no lock" only indirectly (by observing
  that a conflicting prepare was *accepted*), while "lock with no sequence" is detectable by
  observing that prepares on a range are rejected while no sequence exists.
- That asymmetry is worth fixing properly with a small SUT-side assertion in the
  transformation that removes a sequence, checking the resulting metadata has no lock keyed
  to it. Recorded here as the stronger implementation; the workload-side version is the
  fallback that needs no new SUT surface.

## Investigation Log

#### Does `CancelInProgressSequence` unlock ranges in the same transformation that removes the sequence?

- Examined: `transformations/CancelInProgressSequence.java` (presence and role),
  `MultiStepOperation.cancel(ClusterMetadata)` signature — it returns a
  `ClusterMetadata.Transformer`, `LockedRanges.unlock(Key)`, `CMSOperationsMBean`'s
  `cancelInProgressSequences` and `cancelReconfigureCms` entry points,
  `sequences/CancelCMSReconfiguration.java`.
- Found: `cancel()` returning a `Transformer` rather than a `ClusterMetadata` is strong
  evidence for atomicity — a `Transformer` accumulates multiple component changes and is
  applied as one transformation, so unlocking and sequence removal would land in the same
  epoch. The existence of a *separate* `CancelCMSReconfiguration` sequence suggests CMS
  reconfiguration cancellation is itself multi-step, and therefore may not be atomic.
- Not found: the body of `cancel()` in each `MultiStepOperation` subclass, and whether every
  subclass actually calls `unlock`. A subclass that overrides `cancel` without unlocking is
  the concrete bug this property would catch.
- Conclusion: tagged `(partial)`. Evidence favours atomicity for the ordinary sequences and
  is genuinely unclear for CMS reconfiguration. The check tolerates a one-epoch window
  before reporting, so a two-step cancel would not false-positive; the tolerance is
  documented in the checker so it is not mistaken for laziness.
