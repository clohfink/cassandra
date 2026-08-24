# r-sequence-cancelled

## What led to this property

Every multi-step operation (join, leave, move, replace) has a **cancel** path: an operator can abort
a stuck or unwanted in-progress sequence (`nodetool abortbootstrap`, `cancelInProgressSequences`,
`cancelReconfigureCms`), which commits a `CANCEL_SEQUENCE` transformation that must roll the
operation back — release the ranges it locked and revert placements — without orphaning a lock or
leaving a half-applied movement. No driver reached this path, so the rollback was never exercised
under faults.

It is a dense historical bug surface:
- `ea495907e1` — "Make nodetool abortbootstrap more robust"
- `4fb81ea483` — "Add nodetool command to abort failed nodetool cms initialize"
- `95aca49915` — "Avoid NPE during cms initialization abort"

## What it guards

The rollback correctness itself is already covered by the always-on invariants
`b-locked-ranges-match-sequences` and `b-no-overlapping-locked-ranges`, which run on **every**
metadata transition — including the `CANCEL_SEQUENCE` one. If an abort orphaned a `LockedRange` (a
lock with no owning sequence) or left overlapping locks, those `Always` properties would fail on the
cancel transition. This `Sometimes` exists to make that transition **reachable** so the invariants
are non-vacuous over the cancel path; without it, a green result on locked-range safety says nothing
about aborts.

## Code involved

- `tcm/transformations/CancelInProgressSequence.java` and `Transformation.Kind.CANCEL_SEQUENCE`.
- `tcm/log/LocalLog.java` — the SUT-side reachability assertion, keyed on
  `kind == CANCEL_SEQUENCE` at the metadata-publication point.
- `service/StorageService.abortBootstrap(nodeStr, endpointStr)` — requires the target **down**
  ("Can't abort bootstrap ... it is alive"), then commits `CancelInProgressSequence` (for JOIN/
  REPLACE) + `Unregister`.
- `tcm/CMSOperations.cancelInProgressSequences(owner, kind)` / `cancelReconfigureCms` — the generic
  hooks (wired in `Harness.Node`), for future decommission/move/CMS-reconfig aborts.
- Workload `Actions.abortSequence()` + `serial_driver_abort_sequence` — the driver.

## Why SUT-side and not workload-side

Same reasoning as `r-node-replaced`/`r-concurrent-multistep-operations`: `CANCEL_SEQUENCE` is a single
enacted transformation, and observing it at the publication point in `LocalLog` is reliable
regardless of the driver's poll timing — and fires for a cancel however it was initiated.

## Why `Sometimes` and not `Reachable`

The meaningful thing is that a cancellation actually **enacted** (the rollback ran), not that a line
was reached. A cancel request that is rejected (e.g. the sequence progressed past a cancellable step)
must NOT count — keying on the enacted `CANCEL_SEQUENCE` encodes that.

## How the driver reaches it

The realistic "failed bootstrap → abort" flow:

1. Start a spare joining (`joinRing`, fire-and-forget) — a `BootstrapAndJoin` (JOIN) sequence begins.
2. Wait until it is mid-bootstrap (JOINING).
3. **Kill it** — this both stalls the sequence (its owner is gone) and makes it abortable, since the
   SUT refuses to abort a node it still sees as alive.
4. Wait for a peer to mark it down, then `abortBootstrap("", <spare host>)` on a live node — commits
   `CancelInProgressSequence` + `Unregister`.
5. Recycle the spare with wipe-and-restart: the abort Unregistered it, so it re-registers cleanly as a
   fresh spare (no ghost, no pool depletion). Ring size is unchanged throughout — the aborted join
   never joined — so the driver is safe alongside the other churn drivers.

## Open questions

- Extend to `cancelInProgressSequences` on a decommission/move and `cancelReconfigureCms` on a CMS
  reconfiguration. The hooks are wired; whether a leave/move is cancellable at every step (vs only
  early ones) is worth probing — a rejected cancel is itself interesting (`r-commit-rejected`).
