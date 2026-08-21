# r-commit-rejected

## What led to this property

Rejections are linearized *differently* from successes, and that asymmetry has needed fixing.
From `TCM_implementation.md`:

> `Reject`s are not persisted in the log, and are linearized using a read that confirms that
> transformation was executed against the highest epoch.

A separate code path, with a separate correctness argument, that leaves no trace in the log.
The fixes:

- `3e6a551dba` "TCM: Catch up committing node on rejection" (CASSANDRA-19260) — the rejecting
  CMS had to start sending the submitter the entries it was missing, because a rejection is
  itself evidence the submitter's view was stale.
- `802ce7f8b2` "Always send TCM commit failures as Messaging failures."
- `e0766e95bc` "Fix rejectSubsequentInProgressSequence test: cap message sizes for commit
  failures" — rejection responses grew large enough to break messaging.

Three commits on how a rejection is *reported*. A misreported rejection is what makes a client
retry something that already committed, which is `c-commit-survives-cms-membership-change`.

## What it guards

`c-commit-survives-cms-membership-change` most directly: exactly-once semantics under retry
are only tested if rejections and successes both occur, since the hard case is telling them
apart. Also the validation logic inside every `Prepare*`.

## Code involved

- `tcm/AbstractLocalProcessor.java` — where the `Transformation.Result` is examined and a
  `Reject` produced. The assertion site.
- `tcm/PaxosBackedProcessor.java` — the linearizing read for rejections.
- `tcm/Transformation.java` — `Result`, `Success`, `Reject`.
- `tcm/RemoteProcessor.java`, `tcm/Retry.java` — the submitter side.
- `tcm/Commit.java` — response types.

## What a rejection is supposed to look like

CEP-21 gives examples: a replace after a decommission; `ALTER TABLE t` after a committed
`DROP TABLE t`. `TCM_implementation.md` adds: `Register` rejects if a node with the same IP
already exists, and `PrepareJoin` rejects if the computed locked ranges intersect existing
locks or if the node already has an in-progress sequence.

That last pair is what the workload uses, because they are reachable on demand:

- Submit two overlapping-range operations concurrently → one is rejected for lock intersection.
- Submit a second operation for a node that already has a sequence → rejected.

Both are legitimate operator mistakes, so testing them is testing real behaviour rather than
manufacturing an artificial state.

## The tension with `r-concurrent-multistep-operations`

These two properties want opposite outcomes from the same action. This one wants overlapping
concurrent operations (which get rejected); the other wants disjoint concurrent operations
(which get admitted). The workload must therefore vary its range selection rather than always
choosing one or the other — if it always overlaps, concurrency is never achieved; if it never
overlaps, rejections never occur.

This is worth stating explicitly because the natural implementation picks one strategy and
silently starves the other property. The workload randomises between an overlap-seeking and a
disjoint-seeking token choice.

## Why `Sometimes` and where

`Sometimes` with condition `!result.isSuccess()` at the point the processor decides. Not
`Reachable` on the rejection branch: the condition and the branch coincide here, but phrasing
it as `Sometimes(cond)` keeps the assertion's meaning ("a rejection occurred") independent of
how the code is structured, so a refactor that moves the branch does not silently turn the
property into a no-op.

SUT-side rather than workload-side, because a workload-observable rejection is only the subset
that the workload itself caused. Internal rejections — a `Prepare*` rejected during a sequence
advance, or one triggered by an operation another node initiated — are invisible from outside
and are the more interesting cases.

## Implementation notes

- One assertion, one message. Do not add a second `Sometimes` per rejection *reason*; the docs
  warn against reusing one message across unrelated callsites, and the inverse — many messages
  for one property — fragments the report. If per-reason coverage becomes interesting, that is
  a set of distinct properties with distinct names, catalogued separately.
- Include the rejection reason in `Details`. Over a run this produces a histogram of which
  validations actually fire, which is the cheapest available signal on whether the workload is
  exercising the validation surface or hitting the same check every time.
