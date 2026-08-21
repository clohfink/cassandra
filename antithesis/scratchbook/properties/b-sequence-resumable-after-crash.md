# b-sequence-resumable-after-crash

## What led to this property

TCM_implementation.md states the assumption explicitly, and it is unusually strong:

> We make *no assumptions* about liveness of the node between execution of in-progress
> sequence steps. For example, the node may crash after executing `PrepareJoin` but before
> it updates tokens in the local keyspace. So the only assumption we make is that
> `SystemKeyspace.updateLocalTokens` has to be called *before* `StartJoin` is committed.
> Similarly, owned data has to be streamed towards the node *before* it becomes a part of a
> read quorum, so even if the node crashes or is restarted an arbitrary number of times
> during streaming.

"An arbitrary number of times during streaming" is a direct invitation. CEP-21 adds the
recovery model: "cluster metadata simply holds pending states for any node to be executed,"
so a recovering node needs only to catch up and continue.

## Code involved

- `tcm/sequences/InProgressSequences.java` — `resume` / execution driver.
- `tcm/MultiStepOperation.java` — `executeNext()`, `nextStep()`, `advance(CONTEXT)`,
  `finishDuringStartup()` (line 113 — some sequences complete as part of startup),
  `status()`.
- `tcm/sequences/BootstrapAndJoin.java#executeNext` — the local work between committed steps.
- `tcm/sequences/SequenceState.java` — the state returned by `executeNext`.
- `tcm/Startup.java` — resumption on boot.
- `tcm/CMSOperationsMBean.java` — `resumeReconfigureCms()`, `resumeDropAccordTable(tableId)`,
  `cancelInProgressSequences(owner, kind)`. The existence of three separate resume/cancel
  entry points is evidence that sequences do get stuck in practice.

## The four windows

For `BootstrapAndJoin`, the crash windows are between `PrepareJoin`/`StartJoin`,
`StartJoin`/`MidJoin`, `MidJoin`/`FinishJoin`, and after `FinishJoin` but before local
cleanup. Each has different local side effects half-done — local tokens written or not,
streaming partially complete, placements partially applied. `finishDuringStartup()`
existing as a per-sequence flag says some of these are expected to be completed by the
startup path rather than by the normal driver, which is a second dimension of the same
matrix.

Covering that matrix is exactly what Antithesis does and what a dtest cannot: a dtest can
kill a node at a few chosen points, while Antithesis kills at arbitrary instructions and
replays deterministically to reproduce whichever one broke.

## Why this is expressed as `Always` inside `eventually_`

`Sometimes` would be wrong in a way that is easy to get wrong. "Sequences drain" as a
`Sometimes` passes as soon as *one* timeline drains, and Antithesis runs many timelines —
so the one healthy timeline would mask every stuck one. Inside an `eventually_` command,
faults are stopped and killed containers restored, so draining is a *required* outcome and
`Always` is the honest assertion. The `eventually_` command must poll with retries and
health checks, per the Antithesis test-command semantics, because "faults stop immediately"
but containers need time to become operational.

## The killed-node bookkeeping problem

This is the subtlety that decides whether the property is sound. CEP-21 is deliberate that
failure detection must never cancel a sequence: "Because each node's view of liveness is
both subjective and transient, it should never be a trigger for modifying cluster-wide
state," and a single partitioned node halting a bootstrap "would be a severe regression in
terms of stability and operablility." Cancellation is operator-initiated.

Therefore a sequence owned by a node the workload killed and never restarted is *correctly*
stuck, and asserting it drains would be asserting the opposite of the design. The harness
resolves this by policy rather than by weakening the assertion: the workload restarts every
node it stopped before the `eventually_` check runs, so every sequence has a live owner and
draining is genuinely required. Antithesis's own container kills are also restored when the
`eventually_` command begins, which covers the faults the workload did not cause.

## Implementation notes

- The node control agent (see `deployment-topology.md`) must expose "restart all" so the
  `eventually_` command can guarantee the precondition without tracking which nodes it
  killed across a timeline.
- The recovery poll needs a generous initial deadline: draining a join sequence includes
  streaming. Tightening it is a later optimisation and should be done from observed timings,
  not guessed.
- On failure, report `status()` for each remaining sequence — `MultiStepOperation.status()`
  exists for exactly this, and without it a triage report says only "a sequence remained."

## Investigation Log

#### Does a sequence owned by a permanently-gone node count as stuck?

- Examined: CEP-21's failure-handling section (quoted above); TCM_implementation.md's
  no-liveness-assumption paragraph; `CMSOperationsMBean.cancelInProgressSequences`,
  `resumeReconfigureCms`, `resumeReconfigureCms`'s companion `cancelReconfigureCms`;
  `transformations/CancelInProgressSequence.java`; `MultiStepOperation.finishDuringStartup()`.
- Found: unambiguous by design — no, it is not a bug. The protocol requires an operator to
  cancel, and deliberately refuses to infer cancellation from unreachability. The API
  surface confirms this: cancellation is only reachable via explicit operator entry points.
- Not found: nothing outstanding; the design intent is documented in both sources.
- Conclusion: resolved into a harness policy (restart-before-check) rather than an
  assertion caveat, and tagged `(partial)` in the catalog because the *policy's* adequacy is
  what remains unverified — specifically whether a node restarted after a long absence
  resumes its sequence rather than needing operator intervention. That is a real question
  the first runs will answer empirically.
