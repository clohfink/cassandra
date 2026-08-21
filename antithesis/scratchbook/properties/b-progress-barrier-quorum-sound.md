# b-progress-barrier-quorum-sound

## What led to this property

This is the CEP-21 safety theorem, stated as a guarantee about what a lagging coordinator
cannot do:

> a lagging coordinator will **not** be able to collect a quorum for read or write that is
> inconsistent with a quorum obtained using metadata that is up to date.

The progress barrier is the mechanism that makes it true. From CEP-21: "Each step is only
triggered once a majority of the participating nodes have acknowledged the preceding step to
the CMS," with the concrete example that for `{A,B,C}` → `{B,C,X}`, "each step must be
acknowledged by at least 3 of `{A,B,C,X}` before the next step can be started." The
justification: "any two majorities overlap by at least one node and makes the RG itself the
source of truth regarding its composition."

## Code involved

`tcm/sequences/ProgressBarrier.java` (608 lines):

```java
private static final ConsistencyLevel MIN_CL = getProgressBarrierMinConsistencyLevel();
private static final ConsistencyLevel DEFAULT_CL = getProgressBarrierDefaultConsistencyLevel();

public final Epoch waitFor;
public final Location location;
public final LockedRanges.AffectedRanges affectedRanges;
public final Predicate<InetAddressAndPort> filter;

public boolean await()                                    // line 135 — the relaxation loop
public boolean await(ConsistencyLevel cl, ClusterMetadata metadata)   // line 157
public static ProgressBarrier immediate()                 // line 124
public static ConsistencyLevel relaxConsistency(ConsistencyLevel cl)  // line 272
```

The `WaitFor` strategy hierarchy encodes the actual overlap requirement per level:
`WaitForNone` (line 293), `WaitForOne` (306), `WaitForQuorum` (341), `WaitForLocalQuorum`
(380). Each takes `EndpointsForRange writes, EndpointsForRange reads` — both the pre- and
post-step replica sets — and exposes `satisfiedBy(Set<InetAddressAndPort> responded)` and
`waitFor()`. That constructor signature is the property, already factored: soundness means
`satisfiedBy` is true against *both* sets.

`relaxConsistency` walks down: EACH_QUORUM → QUORUM → LOCAL_QUORUM → ONE → NODE_LOCAL.

## Why relaxation is the interesting part

`WaitForNone.satisfiedBy` returns unconditionally and `waitFor()` returns 0. `WaitForOne`
needs a single endpoint. Neither provides quorum overlap in any meaningful sense. So the
soundness of the barrier is entirely contingent on *which level it settled at*, and that is
decided by which nodes responded, which is decided by the network — i.e. by Antithesis.

This is a deliberate liveness/safety trade, not an oversight: without relaxation, a single
unreachable replica would block every topology change, which is the "severe regression in
terms of stability and operablility" CEP-21 rejects elsewhere. The question the property
asks is whether the relaxed level still satisfies the theorem, or whether relaxation below
some point silently gives up on it.

`ProgressBarrier.immediate()` is a separate concern worth noting: some transitions
deliberately do not wait at all. Those callsites are excluded from the property by
construction (the assertion is in `await`), but they are worth an inventory later — a
transition that should have waited and used `immediate()` instead would be invisible here.

## What goes wrong if violated

The theorem fails, and its failure is precisely a consistency violation on the *data* path,
not the metadata path: a coordinator at an old epoch collects what it believes is a quorum
from the old replica set while a coordinator at the new epoch collects a quorum from the new
set, with no intersection. Both succeed. The write is acknowledged and then not returned by
the read. This is the exact scenario CEP-21 says "are possible with the current gossip
implementation" and that TCM exists to eliminate.

## Implementation notes

- `AlwaysOrUnreachable`, not `Always`. Many timelines never advance a sequence far enough to
  evaluate a barrier, and "never ran" must not be a failure. This is the textbook case for
  that assertion type: an optional path that must be correct whenever it runs.
- The assertion belongs at the `return true` in `await(cl, metadata)`, where both the
  responded set and the `WaitFor` instance are in scope. Asserting in `await()`'s loop
  instead would only see the level, not the endpoints.
- `Details` must carry the settled `ConsistencyLevel`, the responded set size, and
  `waitFor()`. Without the level, a failure cannot be distinguished from a legitimate
  operator choice to run with a permissive minimum.

## Investigation Log

#### Is relaxation to `NODE_LOCAL` intended to be sound, or an accepted-risk escape?

- Examined: `ProgressBarrier.java` — `MIN_CL`/`DEFAULT_CL` initialisation from
  `DatabaseDescriptor.getProgressBarrierMinConsistencyLevel()` /
  `getProgressBarrierDefaultConsistencyLevel()`, the `await()` relaxation loop (135–156),
  `relaxConsistency` (272–291), and the four `WaitFor` implementations with their
  `satisfiedBy`/`waitFor` contracts. Also CEP-21's progress-barrier section in full.
- Found: `NODE_LOCAL` is the terminal level of `relaxConsistency`, and `WaitForNone`
  exists with an unconditionally-true `satisfiedBy` — so the code plainly *can* satisfy a
  barrier with zero remote acknowledgements. Both levels are operator-configurable via
  `progress_barrier_min_consistency_level`, which means reaching `NODE_LOCAL` requires an
  operator to have configured it as the minimum. CEP-21 does not discuss relaxation at all;
  its argument assumes majority acknowledgement throughout.
- Not found: any documentation — in CEP-21, `TCM_implementation.md`, or the class javadoc —
  stating the intended safety status of the sub-quorum levels. The gap between CEP-21's
  unconditional theorem and the code's configurable relaxation is unexplained in-tree.
- Conclusion: tagged `(needs human input)`. This is a design-intent question a maintainer
  answers in a sentence and no amount of code reading settles. It materially changes the
  assertion: if sub-quorum relaxation is accepted risk, the assertion must be conditioned on
  `MIN_CL` being quorum-or-stronger, or it will report configuration choices as bugs. The
  harness pins `progress_barrier_min_consistency_level` to a quorum level in
  `cassandra.yaml` so that reaching a sub-quorum barrier is unambiguously a defect, and
  records that pinning as an assumption in `deployment-topology.md`.
