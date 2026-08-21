---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-14
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Source of the claimed guarantees; properties A1–A4, B1–B3, C1–C2, D1 are direct restatements of CEP-21 text.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: 160 commits mined; several properties are regression targets for specific fixed bugs.
---

# Property Relationships

> **Updated post-evaluation.** Six properties added (see `evaluation/synthesis.md` G1–G5). Their
> placement in the clusters below is recorded in "Post-evaluation additions" at the end. Gap 1
> (the control-plane bias) was escalated to the user as bias B1 and remains open; Gap 5
> (`ProgressBarrier.immediate()` callsites) was confirmed by the wildcard lens as the highest
> insight-to-effort item found.

How the 29 properties in `property-catalog.md` relate: which are root causes of which, which
guard which, and where the catalog has redundancy or gaps. This exists so a triage reader can
tell "one bug, four assertions fired" from "four bugs."

## Cluster 1 — The log-integrity core

`a-log-prefix-agreement` · `a-no-gapped-metadata-published` · `a-epoch-monotonic-per-node` ·
`a-metadata-identifier-unique`

**Dominance:** `a-log-prefix-agreement` is the weakest link and the most consequential. If it
fails, `d-schema-agreement-at-same-epoch`, `d-ring-fully-owned`, and
`b-replication-factor-never-under` can all fail as *symptoms* while their own logic is correct.

**Causal chain:** `c-cms-reconfiguration-quorum-overlap` → `a-log-prefix-agreement` →
{`d-schema-agreement-at-same-epoch`, `d-ring-fully-owned`, `b-replication-factor-never-under`}.
Non-overlapping CMS quorums permit two log tails at one epoch; two log tails mean divergent
metadata; divergent metadata means every derived property can be violated. When triaging a
`d-*` failure, check the `c-*` and `a-*` results first — if they also failed, there is one bug,
not four.

**Independence:** `a-metadata-identifier-unique` is *not* downstream of the others. It is a
distinct failure (two independent histories) with a distinct cause (startup/discovery forming a
second CMS), and it can fail while every log-integrity property passes within each half.

**Scope note:** `a-no-gapped-metadata-published` and `a-epoch-monotonic-per-node` are both
local, per-node invariants; `a-log-prefix-agreement` is the cross-node one. All three can be
violated independently, so none subsumes another.

## Cluster 2 — Range movement safety

`b-no-overlapping-locked-ranges` · `b-locked-ranges-match-sequences` ·
`b-replication-factor-never-under` · `d-ring-fully-owned` · `b-sequence-resumable-after-crash`

**Dominance:** `b-no-overlapping-locked-ranges` is the admission gate. Its failure is a
plausible cause of both `b-replication-factor-never-under` and `d-ring-fully-owned`, since two
sequences recomputing placements for one range is exactly how a range ends up under-replicated
or misowned. Triage order: locked-ranges first, placement properties second.

**Mutual reinforcement:** `b-locked-ranges-match-sequences` fails in two directions and each
direction connects elsewhere. An *orphaned lock* causes a stuck cluster, which surfaces as
`b-sequence-resumable-after-crash`. An *unlocked sequence* opens the admission gate, which
surfaces as `b-no-overlapping-locked-ranges`. So this property sits between the two and is the
one to check when either of them fails.

**Cross-cluster:** `b-progress-barrier-quorum-sound` belongs to this cluster mechanically (it
gates sequence steps) but its *consequence* is in the data path, so it is treated in Cluster 4.

## Cluster 3 — CMS membership

`c-cms-membership-never-empty` · `c-cms-reconfiguration-quorum-overlap` ·
`c-commit-survives-cms-membership-change` · `e-cms-accepts-commits-after-recovery`

**Dominance:** `c-cms-reconfiguration-quorum-overlap` is the correctness root;
`c-cms-membership-never-empty` is the availability root. They are genuinely independent — a CMS
can be non-empty with unsound quorums, or sound-but-empty (i.e. gone).

**Downstream:** both feed `e-cms-accepts-commits-after-recovery`, which is the observable
consequence: whichever way CMS membership breaks, the visible symptom is that commits stop. In
triage, `e-cms-accepts-commits-after-recovery` is usually the *first* thing to fail and the
least informative about why; the `c-*` results are the diagnosis.

**Note on `c-commit-survives-cms-membership-change`:** partially overlaps
`a-log-prefix-agreement`. A double-committed transformation appears twice in the log, which is
not a *disagreement* between nodes — every node sees both copies — so `a-log-prefix-agreement`
would pass. The two properties are complementary rather than redundant: one checks agreement
across nodes, the other checks exactly-once within the log.

## Cluster 4 — Metadata's effect on the data path

`b-progress-barrier-quorum-sound` · `r-coordinator-behind-rejection` ·
`d-schema-agreement-at-same-epoch`

This is the thinnest cluster and the catalog's main gap (see below). These three are the only
properties connecting the control plane to actual reads and writes.

**Dominance:** `b-progress-barrier-quorum-sound` is the strongest claim in the entire catalog —
it is CEP-21's headline theorem that a lagging coordinator cannot collect an inconsistent
quorum. `r-coordinator-behind-rejection` covers the detection half of the same story.

**Guard dependency:** `b-progress-barrier-quorum-sound` is `AlwaysOrUnreachable` and is
meaningless unless `r-progress-barrier-relaxed` fires. This is the tightest guard/guarded pair
in the catalog and the two should always be read together.

## Cluster 5 — Derived state

`d-peers-table-matches-directory` · `d-schema-agreement-at-same-epoch`

**Independence:** these are downstream of the log-integrity core but fail for their own
reasons — the listener that writes derived state can be wrong while the log is perfect. That
is in fact the historical pattern: `32755cabfa`, `38512a469c`, and `c484fc511a` are all
listener/derived-state bugs, not log bugs.

**Triage rule:** if a `d-*` property fails while all `a-*` properties pass, the bug is in
enactment (listeners, local db object initialisation), not in the log. If both fail, the log is
the cause. This single distinction is why the `a-*` properties are worth having even though
they check something that "should be true by construction."

## The guard graph

Reachability properties are not findings; they are what makes the safety findings interpretable.
Each arrow means "the guarded property's pass result is uninformative unless the guard fires."

| Guard | Guards | If guard never fires |
|---|---|---|
| `r-concurrent-multistep-operations` | `b-no-overlapping-locked-ranges`, `b-locked-ranges-match-sequences` | Concurrency admission untested; TCM's headline capability unexercised |
| `r-cms-reconfiguration-observed` | `c-cms-reconfiguration-quorum-overlap`, `c-commit-survives-cms-membership-change` | The `AlwaysOrUnreachable` passes vacuously; CEP-21's bounded-divergence argument untested |
| `r-snapshot-catchup-used` | `a-no-gapped-metadata-published` | Only the consecutive-epoch case tested; the one branch allowed to skip epochs never ran |
| `r-progress-barrier-relaxed` | `b-progress-barrier-quorum-sound` | Only `DEFAULT_CL` tested, which is sound by construction; the sub-quorum risk untested |
| `r-commit-rejected` | `c-commit-survives-cms-membership-change` | Exactly-once untested, since the hard case is distinguishing reject from success under retry |
| `r-coordinator-behind-rejection` | (nothing — it is itself the property) | Divergence detection in the request path never exercised |

**Reading rule for reports:** a run where all safety properties pass and any guard did not fire
is a run that needs a longer duration or a more aggressive workload — not a green run. The
guards exist so that "no violations" cannot be mistaken for "no violations possible."

## Workload tensions

Two pairs of properties want opposite things from the same workload action, which the workload
must resolve by randomising rather than by choosing:

- `r-concurrent-multistep-operations` wants **disjoint** concurrent range movements (so they are
  admitted). `r-commit-rejected` wants **overlapping** ones (so they are rejected). A workload
  that always overlaps starves the first; one that never overlaps starves the second.
- `r-progress-barrier-relaxed` wants partitions **sustained** enough to force relaxation.
  `e-cluster-converges-after-faults` and `b-sequence-resumable-after-crash` want faults to
  **stop** so recovery is required. These do not conflict within a timeline — the `eventually_`
  command creates the quiet period — but they do mean the driver phase must be long enough for
  relaxation to occur before recovery is checked.

A third tension is configuration rather than workload:
`b-progress-barrier-quorum-sound` is only interpretable if
`progress_barrier_min_consistency_level` is pinned to a quorum level, but that pinning narrows
the range `r-progress-barrier-relaxed` can explore to a single step. See
`r-progress-barrier-relaxed.md` for the trade and the second-configuration option.

## Redundancy audit

Deliberate overlap, kept because the properties fail distinguishably:

- `a-log-prefix-agreement` vs. `c-cms-reconfiguration-quorum-overlap` — symptom vs. root cause.
  Keeping both localises the bug on first failure.
- `a-epoch-monotonic-per-node` uses two assertions (SUT-side at the CAS, workload-side across
  restarts). Neither subsumes the other: the first catches sub-poll-interval regressions, the
  second catches cross-restart ones.
- `e-cluster-converges-after-faults` asserts both epoch equality and directory equality; the
  latter would also be caught by `a-log-prefix-agreement`. Cheap, and catching it in the quiet
  period gives a cleaner signal than catching it mid-fault.

No property in the catalog is strictly subsumed by another.

## Post-evaluation additions

Where the six new properties sit relative to the clusters above.

### Cluster 1 (log integrity) gains two `Unreachable` tripwires

`a-log-processing-never-concurrent` and `a-log-processing-never-halts` are both in
`LocalLog.processPendingInternal`, alongside the two `a-no-gapped-metadata-published` assertions
and the `r-snapshot-catchup-used` one — five assertions in one method, which is proportionate
given it is the single point where metadata is published.

**New causal edges:**

- `a-log-processing-never-concurrent` → `a-epoch-monotonic-per-node`. Concurrent processing means
  two threads through `notifyPreCommit`; the CAS loser has already initialised objects for metadata
  never published. If both fire, the concurrency violation is the cause.
- `a-log-processing-never-halts` → `e-cluster-converges-after-faults`. A halted node is frozen
  forever, which surfaces as non-convergence. This is the clearest cause/symptom pair added by the
  evaluation: previously a halt was only visible as the symptom, several inferential steps from the
  cause.
- `a-log-processing-never-halts` ← `a-log-prefix-agreement`. A transformation that succeeded on the
  CMS but rejects locally means either the base state differed (log disagreement) or the
  transformation is not pure. If both fire, log disagreement is the cause.

### Cluster 3 (CMS) gains the initialization window

`c-initialization-abort-recoverable` covers the state every other Category A and C property guards
itself out of via `epoch >= FIRST`.

**Shared mechanism, not shared property:** it and `a-metadata-identifier-unique` both require the
`wipe-and-rejoin` workload action, because Antithesis injects no faults before `setup_complete` so
initialization cannot be faulted on the natural path. Neither property would justify that machinery
alone; together they do. If the wipe-and-rejoin action is dropped, **both** become permanent
no-ops that read as passes — worth stating because that is a silent failure of two properties from
one omission.

**Convergent failure:** `c-initialization-abort-recoverable` and `a-no-gapped-metadata-published`
meet at the unresolvable-gap-at-`Epoch.FIRST` case documented in `LocalLog.java:513-517`. That gap
arises from entry reordering during initialization, which is precisely what a partition during
initialization produces.

### Cluster 4 (data path) gains its second real member

`d-prepared-statement-not-stale` is only the second property in the catalog that observes
client-visible wrong answers (the first, `r-coordinator-behind-rejection`, may not fire per
`evaluation/antithesis-fit.md` F1.4). It does not close bias B1, but it is a genuine instance of the
missing category obtained cheaply.

**Shared open question with `d-schema-agreement-at-same-epoch`:** both hinge on whether the
relevant listener runs in `notifyPreCommit` (atomic with the epoch) or `notifyPostCommit` (lagging
it). One code read resolves both. If pre-commit, both properties are cheap regression guards; if
post-commit, both are live hypotheses.

### Cluster 5 and availability

`e-cluster-serves-requests-during-churn` completes the `e-*` set into a three-way span: usable
*during* faults, converged *after*, changeable *after*. It shares a failure mode with
`d-ring-fully-owned` — an unowned range makes requests for those tokens fail — where
`d-ring-fully-owned` is the diagnosis and availability is the symptom.

### Category H stands alone

`h-all-nodes-compared` has no SUT relationship; it guards the *validity* of six cross-node
properties (`a-log-prefix-agreement`, `a-metadata-identifier-unique`,
`c-cms-membership-never-empty`, `c-initialization-abort-recoverable`,
`d-schema-agreement-at-same-epoch`, `e-cluster-converges-after-faults`). It belongs in the guard
graph above conceptually, but its failure means those six results are *untrustworthy* rather than
*vacuous* — a different and worse condition, since a vacuous pass at least reflects a real absence
of evidence while an untrustworthy pass reflects evidence that was silently discarded.

## Gaps

1. **The data path is barely covered.** Three properties touch reads and writes, and only
   `r-coordinator-behind-rejection` observes client-visible behaviour. CEP-21's coordinator-side
   re-check — "it checks if collected replica responses still correspond to the consistency level
   query was executed at" — has **no property at all**. Covering it properly needs a
   linearizability workload with a history checker (a Jepsen-style register or Antithesis's
   documented ring/chain-of-blocks patterns), which is a different workload shape from metadata
   churn. This is the largest and clearest expansion.
2. **The gossip→TCM upgrade path is entirely uncovered** and is the densest historical bug area
   (`4318e74180`, `cdfce6b4ac`, `417bb21d2e`, `db94321d71`, `46b90364da`, `1ed52038ce`). It needs
   a mixed-mode deployment, so it is a second harness.
3. **Multi-datacenter.** `EACH_QUORUM` progress barriers, per-DC CMS RF
   (`reconfigureCMS(Map<String,Integer>)`), and DC-aware placement are unexercised by the
   single-DC topology.
4. **Accord's TCM coupling** (consensus migration, `AccordMarkStale`,
   `ReconfigureAccordFastPath`, `DropAccordTable`) is excluded deliberately for attributability,
   but it is real TCM surface with its own sequences.
5. **No property covers `ProgressBarrier.immediate()` callsites.** Transitions that deliberately
   skip the barrier are excluded by construction from `b-progress-barrier-quorum-sound`. A
   transition that *should* have waited but used `immediate()` would be invisible to the entire
   catalog. Auditing those callsites is cheap and worth doing.
