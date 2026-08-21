# r-progress-barrier-relaxed

## What led to this property

`ProgressBarrier.await()` (line 135) does not simply wait at one consistency level. It starts
at `DEFAULT_CL` and walks down via `relaxConsistency()` toward `MIN_CL`:

```java
ConsistencyLevel currentCL = DEFAULT_CL;   // line 142
...
ConsistencyLevel prev = currentCL;         // line 148
```

with `relaxConsistency` (line 272) mapping EACH_QUORUM → QUORUM → LOCAL_QUORUM → ONE →
NODE_LOCAL.

Relaxation only happens when nodes do not respond — i.e. only under fault. On a healthy
network the barrier is satisfied at `DEFAULT_CL` every time and the relaxation code never
runs. That means conventional integration tests, which run on healthy networks, essentially
never execute it. Antithesis-injected partitions are the only practical way to reach it.

## What it guards

`b-progress-barrier-quorum-sound`, which is an `AlwaysOrUnreachable` on the barrier's
soundness. If relaxation never happens, that property only ever evaluates the
`DEFAULT_CL` case — the case that is sound by construction — and passing it says nothing about
the sub-quorum levels where the interesting risk lives.

This pairing is the clearest example in the catalog of why reachability properties belong
alongside safety ones: `b-progress-barrier-quorum-sound` passing without
`r-progress-barrier-relaxed` firing is not evidence of anything.

## Code involved

`tcm/sequences/ProgressBarrier.java`:

- `MIN_CL` / `DEFAULT_CL` from `DatabaseDescriptor.getProgressBarrierMinConsistencyLevel()` and
  `getProgressBarrierDefaultConsistencyLevel()` (lines 87–88).
- `await()` (135) — the relaxation loop; the assertion site.
- `await(ConsistencyLevel cl, ClusterMetadata metadata)` (157) — a single attempt.
- `relaxConsistency(ConsistencyLevel)` (272).
- The `WaitFor` hierarchy: `WaitForNone` (293), `WaitForOne` (306), `WaitForQuorum` (341),
  `WaitForLocalQuorum` (380).
- `ProgressBarrier.immediate()` (124) — barriers that do not wait at all; excluded, since the
  assertion is in `await`.

## The window is genuinely narrow

Relaxation needs a partition that is *sustained enough* to prevent satisfaction at the current
level, but *not so severe* that the sequence gives up or the node loses its CMS connection
entirely. Too little partition: satisfied at `DEFAULT_CL`. Too much: the operation fails
outright and relaxation is moot.

Hitting a window bounded on both sides is precisely the capability Antithesis has and scripted
tests do not — and it is why this property is worth stating rather than assuming it happens.

## Interaction with the harness's configuration pinning

`deployment-topology.md` pins `progress_barrier_min_consistency_level` to a quorum level, so
that reaching a sub-quorum barrier is unambiguously a defect rather than an operator choice
(see the investigation log in `b-progress-barrier-quorum-sound.md`).

That pinning has a direct cost here: it *narrows the relaxation range*. With `DEFAULT_CL` at
EACH_QUORUM and `MIN_CL` at QUORUM there is exactly one relaxation step available, so this
property fires only on that single transition. The trade is deliberate — a sound
`b-progress-barrier-quorum-sound` signal is worth more than a wider relaxation range — but it
should be revisited: running a second configuration with a permissive `MIN_CL` would explore
the sub-quorum levels, at the cost of needing the maintainer answer recorded as
`(needs human input)` in `b-progress-barrier-quorum-sound.md` before the results could be
interpreted.

## Implementation notes

- SUT-side, in the relaxation loop, condition `currentCL != DEFAULT_CL`. Not workload-side:
  nothing external can observe which consistency level a barrier settled at.
- Include both `DEFAULT_CL` and the reached level in `Details`. Over a run this shows how far
  down the ladder the harness actually gets, which is the number that says whether the
  partition profile is producing the intended pressure.
- One assertion at the loop, not one per level. Per-level coverage would be a set of distinct
  properties; as a single `Sometimes` the message stays "relaxation occurred," which is the
  guard `b-progress-barrier-quorum-sound` needs.
