# b-replication-factor-never-under

## What led to this property

CEP-21's central promise about movements: committed changes "logically take effect across
the cluster in a way that preserves advertised replication factor and quorum consistency,"
supporting "multiple concurrent additions, removals and replacements without compromising
durability, correctness or availability."

And commit `63c6261856`: "Reimplement `ClusterMetadata::writePlacementAllSettled` to step
through InProgressSequences to determine state when finished" (CASSANDRA-19193). The
original implementation of "what will the write placements be once everything settles" was
wrong enough to need reimplementation by stepping through the sequences. That function is
how the system reasons about whether a movement is safe to proceed, so its correctness is
this property.

## Code involved

- `tcm/ownership/DataPlacements.java`, `tcm/ownership/ReplicaGroups.java`,
  `tcm/ownership/PlacementForRange` (per-range read/write endpoint sets).
- `tcm/ClusterMetadata.java` — `writePlacementAllSettled`.
- `tcm/transformations/ApplyPlacementDeltas.java` — how each sequence step mutates placements.
- `tcm/sequences/BootstrapAndJoin.java`, `UnbootstrapAndLeave.java`, `Move.java`,
  `BootstrapAndReplace.java` — the step sequences producing the deltas.
- `tcm/sequences/LeaveStreams.java`, `UnbootstrapStreams.java`, `RemoveNodeStreams.java`,
  `DataMovements.java` — the streaming half.

## The design intent, which is what makes the property non-trivial

The phased design deliberately runs at **more** than RF during a movement. From
TCM_implementation.md: "owned data has to be streamed towards the node *before* it becomes a
part of a read quorum." And CEP-21 on the bootstrap ordering rationale: writes continue to
the departing replica so that "by the time we stop writing to the node giving up the range,
there is no coordinator that may attempt reading from it" without having seen the relevant
epoch. This is what `PendingRanges` used to accomplish: "Replication factors for writes must
be temporarily expanded during range movements."

So the correct expectation is `writeReplicas >= RF` throughout, with equality only in the
settled state. The bug shape is a step ordering where the shrink is applied before the grow —
a transient dip to RF-1 that no conventional test would catch because it exists only for the
duration of one step, and only when that step is interleaved with something else.

## What goes wrong if violated

A range at RF-1 write replicas plus one node loss equals acknowledged-write loss at
`QUORUM`. It is invisible at the time: the write succeeds, the client is told it succeeded,
and the data is simply on fewer replicas than the operator believes. Detection happens
later, during a repair or an unrelated node failure, with no way to attribute it to the
movement that caused it.

## Why this is hard to check correctly, and how to keep it honest

Two distinct sources of under-replication must not be conflated:

1. **Protocol-caused** — the sequence's step ordering produced a dip. This is the bug.
2. **Operator-caused** — the workload decommissioned so many nodes that RF simply cannot be
   satisfied by the surviving set. This is not a bug; TCM is expected to *reject* such an
   operation ("a decommission that would leave required replication factors could not be
   satisfied may be rejected"), and if it accepts one, that is a different property.

The checker therefore computes `min(RF, liveRegisteredNodeCount)` as its floor and records
which of the two regimes it is in. Conflating them would produce a stream of false failures
the moment the workload gets aggressive, and the usual response to noisy assertions is to
weaken them until they find nothing.

## Implementation notes

- Probe keyspace with known RF created by the `first_` command, so RF is a constant the
  checker knows rather than something it has to infer.
- Read placements per node; a node mid-catch-up legitimately has an older placement view.
  The property is per-node-per-epoch: at any epoch a node holds, that epoch's placements
  must satisfy the floor. That formulation removes lag as a source of noise entirely.
- `system_views.cluster_metadata_directory.tokens` plus the probe keyspace's replication
  settings are sufficient to reconstruct expected replica counts without new SUT surface.

## Investigation Log

#### Where exactly does `UnbootstrapAndLeave` drop the leaving node from write placements?

- Examined: `sequences/UnbootstrapAndLeave.java`, `LeaveStreams.java`,
  `UnbootstrapStreams.java`, `RemoveNodeStreams.java` (class structure and role);
  `MultiStepOperation.nextStep()` / `advance()` contract; TCM_implementation.md's
  ProgressBarrier section; CEP-21's bootstrap-ordering rationale quoted above.
- Found: the *direction* of the invariant is documented and unambiguous — grow before
  shrink, with a progress barrier between steps ensuring a majority of affected-range owners
  learned the previous step. CEP-21 gives the concrete `{A,B,C}` → `{B,C,X}` example
  requiring acknowledgement by 3 of `{A,B,C,X}` per step.
- Not found: the per-step placement deltas for leave. Determining the exact instant of the
  drop requires reading `ApplyPlacementDeltas` together with the deltas each leave step
  emits — more than discovery-scope reading.
- Conclusion: tagged `(partial)`. The `>= min(RF, liveNodes)` formulation is correct under
  any step ordering that respects grow-before-shrink, so implementation is unblocked. If the
  assertion fires, the first triage step is to read those deltas — noted so the next reader
  does not repeat this search.
