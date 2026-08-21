# e-cluster-serves-requests-during-churn

Added by evaluation gap G4 (`evaluation/wildcard.md` F4.2).

## What led to this property

An absence, noticed by asking what the catalog would *not* catch. Every safety property is of the
form "nothing bad is in the metadata." The two `e-*` recovery properties check state after faults
stop. Nothing checks that the cluster is usable *while* the interesting things are happening.

The concrete scenario that would pass everything else: a TCM bug leaves placements in a state
where no coordinator can construct a valid replica plan, so every read and write is rejected for
the entire fault-injected phase. Then `eventually_` fires, faults stop, the cluster settles,
`e-cluster-converges-after-faults` and `e-cms-accepts-commits-after-recovery` both pass, and the
metadata was internally consistent the whole time. Fully green, total outage.

The Antithesis test-command reference names this use case for `anytime_` commands directly:

> Availability monitoring: "it's possible to make a read without timing out".

## Why `Sometimes` and not `Always`

This is the assertion-type decision that matters most here, and getting it backwards would make
the property useless in either direction.

`Always("every request succeeds")` would be asserting the absence of the faults Antithesis exists
to inject. Under a partition, requests at `QUORUM` are *supposed* to fail — that is correct
behaviour, and asserting otherwise would fire on every timeline.

`Sometimes("a read and a write both completed")` states the real claim: the cluster is not
*continuously* unusable. Faults come and go; between them, the system must work. That is a
meaningful semantic condition, which is what the guidance reserves `Sometimes` for — as opposed to
`Reachable`, which would only mark that the request code ran.

## What it does not claim

Worth being precise, because the property is easy to over-read. It does not claim:

- any particular availability level;
- that requests succeed during any *specific* fault;
- that the data returned is correct (that would be the linearizability workload recorded as
  bias B1 in `evaluation/synthesis.md`).

It claims only that a `QUORUM` read and a `QUORUM` write both completed at least once during the
driver phase. That is a low bar deliberately: it cannot false-positive, so it will never be the
property that gets weakened for noise, and it catches the total-outage class that nothing else
covers.

## Code involved

Nothing SUT-side. This is purely a workload check against the probe keyspace, run in the same
`anytime_` checker that evaluates the cross-node invariants — so it costs one extra read and one
extra write per cycle.

Relevant server-side machinery it indirectly exercises: replica plan construction from
`DataPlacements`, and the coordinator's post-response placement re-check that CEP-21 describes
("it checks if collected replica responses still correspond to the consistency level query was
executed at"). This property does not *verify* that re-check — it only notices if the path is
entirely unusable.

## Implementation notes

- Use the load-balanced session, not the pinned single-host sessions. Pinning to a partitioned
  node would make failure expected and the property would rarely fire.
- `QUORUM` for both operations. `ONE` would be too easy to satisfy and would not exercise replica
  plan construction meaningfully; `ALL` would fail under any single-node fault and the property
  would rarely fire.
- Write then read the same key, but do **not** assert the read returns the write — that is a
  correctness claim this property is not making, and asserting it here would create a weak,
  ambiguous version of the linearizability property that B1 calls for. Keep the scope honest: this
  is an availability probe.
- Record the per-cycle success/failure counts as run metadata. The assertion's pass bar is "at
  least once," but the ratio over a run is the number that says whether availability was
  reasonable or barely scraped by — and that ratio is the input to deciding whether the stronger
  per-N-cycles form (see the open question) is calibratable.

## Relationship to other properties

- Complements `e-cluster-converges-after-faults` and `e-cms-accepts-commits-after-recovery`, which
  cover the quiet period. Together the three span: usable during faults, converged after,
  changeable after.
- Partially mitigates bias B1 by putting *some* client-visible behaviour under assertion, without
  pretending to close it.
- Its failure mode overlaps `d-ring-fully-owned`: a ring with an unowned range would make requests
  for those tokens fail. If both fire, `d-ring-fully-owned` is the diagnosis and this is the
  symptom.
