---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-14
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Guarantee statements; the source of the catalog's claims and of the bias finding.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: Bug history; the coverage checklist the Coverage Balance lens walked.
---

# Evaluation Synthesis

Four lenses run in single-agent mode over the 23-property catalog. 19 findings, categorized
below. Evidence files: `antithesis-fit.md`, `coverage-balance.md`, `implementability.md`,
`wildcard.md`.

Headline: the catalog is well-grounded and correctly typed, but had **three properties specified
against observation surfaces that do not support them**, **one property whose main fault scenario
this harness cannot reach**, and **one structural bias** that no refinement can fix.

## Bias — requires human judgment

### B1 — The catalog tests the control plane, not the guarantee the control plane exists to provide

**Source:** `wildcard.md` F4.1, corroborated by `coverage-balance.md`'s component-spread analysis
(request path is the thinnest area) and `antithesis-fit.md` F1.4 (the one client-visible property
may not fire).

20 of 23 properties concern metadata as an object. A TCM implementation could pass this catalog
entirely green while losing acknowledged writes — if the coordinator's post-response placement
re-check is broken, a write acknowledged at `QUORUM` during a range movement can land on replicas
that no longer own the range, and nothing in the catalog looks at data.

CEP-21 names that re-check as part of the design: after collecting responses the coordinator
"compares its `Epoch` with the `Epoch` that was used to construct the `ReplicaPlan`... it checks if
collected replica responses still correspond to the consistency level query was executed at." No
property covers it.

The bias is explicable: research was scoped to "TCM," TCM is a package, and scoping by package
produced a catalog about that package. The guarantee, however, is a data-path guarantee whose
consuming code lives outside `tcm/`.

**Why it is a bias and not a gap:** closing it needs a second workload shape — a
linearizability/register workload with a history checker — comparable in size to the entire
harness built here, and it partially overlaps existing Cassandra testing (`test/simulator`,
`ci/harry_simulation.sh`, and the Harry commits `9fe1a977b5` / `0989a219ad`). Whether to build it
or to deliberately accept a control-plane harness and rely on Harry for the data path is a
scoping call with real cost either way.

**Action:** escalated to the user. Recorded here and as Gap 1 in `property-relationships.md`.
Not resolved in this pass. Two partial mitigations *were* applied (see G5 and R4), which reduce
the blast radius without closing the bias.

## Gaps — filled in this pass

### G1 — CMS initialization abort and the empty-placements state

**Source:** `coverage-balance.md` F2.1. Three fixed bugs with no property: `ec7794f20f` (NPE when
meta keyspace placements empty pre-init), `2bc24da841` (allow empty placements on deserialize),
`95aca49915` (NPE during initialization abort). Plus a whole operator surface —
`initializeCMS(List<String> ignore)`, `abortInitialization(String initiator)`, and `4fb81ea483`'s
nodetool command. Every Category A and C property explicitly guards itself out of this window
with `epoch >= FIRST`, so the catalog was systematically blind to it.

**Action:** added `c-initialization-abort-recoverable`.

### G2 — Prepared-statement staleness after schema change

**Source:** `coverage-balance.md` F2.2. Three fixed bugs on one seam: `9bf1680b1f`
(CASSANDRA-20116, invalidation race), `1a6b8e0628` (invalidate on every table metadata change),
`740879d5a0` (don't clear cache on `cms initialize`). The seam is identified precisely in
`d-schema-agreement-at-same-epoch.md`: `notifyPreCommit` fires before the CAS and
`notifyPostCommit` after, so listener-maintained state lags the published epoch. This is also a
client-visible wrong-answer path, which partially mitigates B1.

**Action:** added `d-prepared-statement-not-stale`.

### G3 — No `Unreachable` assertions, with two documented-impossible states available

**Source:** `coverage-balance.md` F2.3. Distribution was 12 `Always` (+3 in quiet periods),
2 `AlwaysOrUnreachable`, 6 `Sometimes`, 0 `Reachable`, 0 `Unreachable`. Zero `Reachable` is
deliberate and fine. Zero `Unreachable` left two states unchecked that the code itself declares
impossible:

- `LocalLog.java:565` `IllegalStateException("CAS conflict...")` — reaching it means the
  single-caller contract on `processPendingInternal` was violated. This also converts the
  `(needs human input)` open question in `a-no-gapped-metadata-published.md` into an empirical
  check.
- `LocalLog.java:532/538` `StopProcessingException` — a node permanently stops applying log
  entries; its message blames configuration divergence, impossible in a homogeneous harness.

Both currently surface only as a log line inside the `catch (Throwable t)` at line 575, so today
a violated core invariant produces no test failure anywhere.

**Action:** added `a-log-processing-never-concurrent` and `a-log-processing-never-halts`.

### G4 — Nothing asserts availability during churn

**Source:** `wildcard.md` F4.2. Every safety property is "nothing bad is in the metadata"; the
`e-*` properties check recovery only *after* faults stop. A cluster that rejects every request
throughout the entire driver phase and recovers at the end passes the catalog. The Antithesis
test-command reference names this as a canonical `anytime_` use: "it's possible to make a read
without timing out."

**Action:** added `e-cluster-serves-requests-during-churn`. Cheap — a few lines in a checker the
workload already runs — and it partially mitigates B1 by putting *some* client-visible behaviour
under assertion.

### G5 — The harness cannot distinguish "partitioned" from "down", and hides it

**Source:** `wildcard.md` F4.3, raised as an open question in `deployment-topology.md`. At least
six checkers say "only assert over nodes that answered." Excluding a partitioned node is correct;
excluding a node that died is how the harness silently degrades into one that always passes. Both
look identical from the workload — a timeout — and the exclusion would be invisible in the report.

**Action:** added `h-all-nodes-compared`, a reachability property on the *harness* asserting that
at least sometimes all five nodes were included in a cross-node comparison. Exclusion becomes
visible rather than silent.

## Refinements — applied in this pass

| ID | Source | Property | Change |
|---|---|---|---|
| R1 | `implementability.md` F3.1 | `b-replication-factor-never-under`, `d-ring-fully-owned` | Respecified against `StorageServiceMBean.getRangeToEndpointWithPortMap` / `getPendingRangeToEndpointWithPortMap` instead of reconstructing placements from tokens. The original spec required reimplementing Cassandra's placement algorithm in the workload, which would have tested the reimplementation. Simpler *and* stronger. |
| R2 | `implementability.md` F3.2 | `b-locked-ranges-match-sequences` | `LockedRanges` is exposed by no virtual table and no MBean method (verified against `SystemViewsKeyspace` registrations and every `CMSOperationsMBean` method). Moved SUT-side, asserting the bijection in the transformation that removes a sequence. The weak workload-side proxy is dropped — a weak check on a P0 property is worse than an honest absence. |
| R3 | `implementability.md` F3.3 | `a-log-prefix-agreement` | Recorded an explicit fallback: if `dumpLog` proxies the distributed table, the cross-node comparison is vacuous, so fall back to a SUT-side check in `processPendingInternal` comparing the entry about to be enacted against any entry already persisted at that epoch. The catalog's most consequential property no longer has a coin-flip status with no plan B. |
| R4 | `antithesis-fit.md` F1.1 + `implementability.md` F3.4 | `a-metadata-identifier-unique` | Antithesis angle rewritten. Faults are not injected until `setup_complete`, so partition-during-initial-CMS-formation is unreachable by construction. Reachable instead via a `wipe-and-rejoin` workload action that pushes a node back through `Startup`/`Discovery` during the fault phase, with `unregisterLeftNodes` first so the rejoin is a real discovery rather than a legitimate rejection. |
| R5 | `antithesis-fit.md` F1.2 | `d-ring-fully-owned` | Value bound to evaluation context: the checker records whether `inProgressSequences` was non-empty at evaluation time, and the run reports churn vs. settled evaluation counts. As stated it was largely unit-testable; its Antithesis value is only in the interrupted-concurrent-movement case. |
| R6 | `antithesis-fit.md` F1.3 | `d-peers-table-matches-directory` | Expected value raised (Cassandra ships a repair tool for this drift — `c484fc511a`), and the `NodeState`-filter open question promoted to blocking, since the current mid-movement exclusion is a workaround for it that costs more coverage than it appears to. |
| R7 | `antithesis-fit.md` F1.4 + `implementability.md` F3.5 | `r-coordinator-behind-rejection` | Workload must construct the materiality condition deliberately (pin a single-host session to a node partitioned from the CMS, run DDL on the exact probe table that session queries) rather than hoping for it, and must resolve the observation mechanism (`ExceptionsTable` leading) before implementation. A `Sometimes` that cannot be observed is indistinguishable from a state never reached, which reads as a pass. |
| R8 | `coverage-balance.md` F2.4 | `c-commit-survives-cms-membership-change` | Rejection safety folded in as an explicit sub-condition: a *rejected* transformation must appear at no epoch. Reuses the tag ledger already required; no new property, no report fragmentation. |
| R9 | `wildcard.md` F4.4 | `c-cms-membership-never-empty`, `b-replication-factor-never-under` | Workload must decline to submit operations that would drop the ring below RF or the CMS below its configured RF, and log when it declines. Keeps P0 assertions sharp instead of letting operator-caused states create noise that would eventually get them weakened. |
| R10 | `wildcard.md` F4.5 | run metadata | Log length per timeline and snapshot occurrence recorded as run metadata. Unbounded log growth would surface as an `e-cluster-converges-after-faults` failure and be misdiagnosed as a catch-up bug. |
| R11 | `implementability.md` F3.5 | topology | Workload container requirement added: one single-host session per Cassandra node in addition to the load-balanced session, so queries can be deliberately coordinated through a known-lagging node. |

## Noted, not actioned

- **`antithesis-fit.md` F1.5** — `b-progress-barrier-quorum-sound`,
  `c-cms-reconfiguration-quorum-overlap`, and `b-sequence-resumable-after-crash` each falsify a
  CEP-21 *timing* claim that has no mechanical check today (CEP-21 records that a TLA+ spec of
  epoch visibility "was explored but omitted"). No change needed, but their guards
  (`r-progress-barrier-relaxed`, `r-cms-reconfiguration-observed`) should be treated as blockers
  for trusting the harness: if those never fire, its most valuable properties are inert.
- **`wildcard.md` "What is odd"** — the `ProgressBarrier.immediate()` callsites. Every use is a
  per-callsite author judgement that a transition needs no majority acknowledgement, untested and
  invisible to `b-progress-barrier-quorum-sound` by construction. An inventory is a code-reading
  task, not a testing task; highest insight-to-effort ratio found in the evaluation. Left as
  Gap 5 in `property-relationships.md`.

## Post-evaluation catalog

29 properties: 23 original + 6 added (G1–G5, with G3 contributing two).

| Type | Count |
|---|---|
| `Always` (safety) | 15 |
| `Always` inside quiet-period commands | 3 |
| `AlwaysOrUnreachable` | 2 |
| `Unreachable` | 2 |
| `Sometimes` | 7 |
| `Reachable` | 0 (deliberate — `Sometimes` on a meaningful condition preferred throughout) |

## Open Questions

- B1 is unresolved and is a user decision. Everything else in this synthesis is applied.
- Three implementability questions must be resolved before the corresponding checkers are
  written, and all three would silently produce vacuous passes if guessed wrong: `dumpLog`'s
  data source (R3), `CoordinatorBehindException`'s observability (R7), and whether
  `getPendingRangeToEndpointWithPortMap` is still populated under TCM (R1 — TCM replaced
  `PendingRanges` with placement-based expansion, so it may return empty).
- Whether `CMSOperations` registers its MBean on a node started with
  `-Dcassandra.join_ring=false`. If not, both spares are unobservable and every cross-node check
  silently drops 2 of 5 nodes — which is exactly the failure mode `h-all-nodes-compared` was
  added to make visible.
