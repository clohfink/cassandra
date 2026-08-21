---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-14
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Used to question whether the catalog tests what TCM is *for* rather than what TCM *is*.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: Checked for bug classes that no attention focus would naturally surface.
---

# Evaluation Lens 4 — Wildcard

Other lenses in this run: **Antithesis Fit** (is each property in the platform's sweet spot),
**Coverage Balance** (is this the right set against the SUT risk table), **Implementability**
(can each be observed and driven). This lens starts where those end.

## Findings

### F4.1 — The catalog tests what TCM *is*, not what TCM is *for*

**Scope:** catalog-wide bias. This is the finding I would escalate above all others.

Twenty of twenty-three properties are about metadata as an object: is the log ordered, do nodes
agree on it, are placements well-formed, does the CMS survive. Three touch reads and writes, and
one of those may not fire (per F1.4).

**A TCM implementation could pass this entire catalog green while losing acknowledged writes.**
Concretely: every log-integrity property holds, all nodes agree on every epoch, placements are
contiguous and at RF — and the coordinator's post-response placement re-check is broken, so a
write acknowledged at `QUORUM` during a range movement lands on replicas that no longer own the
range. Nothing in the catalog looks at data.

CEP-21 is explicit that this re-check is part of the design:

> After coordinator has collected enough responses, it compares its `Epoch` with the `Epoch` that
> was used to construct the `ReplicaPlan` for the query it is coordinating. If epochs are
> different, it checks if collected replica responses still correspond to the consistency level
> query was executed at.

`property-relationships.md` records this as "Gap 1" and calls it "the largest and clearest
expansion." I think that undersells it. It is not an expansion of the catalog; it is the
catalog's purpose. Everything else is a proxy for it.

The bias is structural and explicable: the research was scoped to "TCM," and TCM is a package
under `src/java/org/apache/cassandra/tcm/`. Scoping by package produced a catalog about that
package. But the *guarantee* TCM exists to provide is a data-path guarantee, and the request-path
code that consumes cluster metadata lives outside that package.

**Why this needs human judgment rather than a fix:** closing it means a second workload shape —
a linearizability/register workload with a history checker, of the kind Antithesis documents in
its ring-test and chain-of-blocks resources and its key-value property catalog. That is
comparable in size to everything built so far, and it partially overlaps existing Cassandra
testing (Harry, the in-tree simulator, `ci/harry_simulation.sh` exists in this repo). Whether to
build it, or to deliberately accept a control-plane-only harness and rely on Harry for the data
path, is a scoping decision with real cost either way.

### F4.2 — Nothing asserts the cluster stays *available* during churn

**Scope:** catalog-wide gap, and cheap to close. Every safety property is of the form "nothing
bad is in the metadata." No property says "the cluster is still answering."

A TCM bug that makes every coordinator reject every request — say, a placement state where no
replica plan can be constructed — would violate nothing in the catalog. `e-*` properties check
recovery *after* faults stop, so a cluster unavailable throughout the entire driver phase and
healthy at the end passes.

The Antithesis test-command reference names this exact use for `anytime_` commands:
"Availability monitoring: 'it's possible to make a read without timing out'."

This is a few lines in the checker the workload already runs, and it converts a whole class of
silent failure into a visible one. Unlike F4.1 it needs no new workload shape.

### F4.3 — The harness cannot distinguish "partitioned" from "down", and several checkers depend on that distinction

**Scope:** catalog-wide, cross-cutting. `deployment-topology.md` raises this as an open question.
I want to escalate it, because it is not a detail — it is a correctness property *of the harness*,
and it cross-cuts every lens.

At least six checkers say "only assert over nodes that answered." If a node is unreachable
because of an injected partition, excluding it is correct. If it is unreachable because it
crashed on an assertion violation and is no longer serving, excluding it **hides the failure the
harness exists to find**. Both look identical from the workload: a timeout.

The control agent's `status` endpoint helps, but it is on the same network and subject to the same
partitions, so it cannot resolve the ambiguity in the case that matters most (a total partition of
that node).

This is the mechanism by which a harness silently degrades into one that always passes, and it
would not be visible in any report — the run would show green properties and no indication that
two of five nodes were excluded from every check.

**Suggested action:** make exclusion *visible* rather than trying to eliminate it. Every checker
should record how many nodes it evaluated, and the run should assert
`Sometimes(evaluatedNodes == totalNodes)` — a reachability property on the *harness*, confirming
that at least sometimes all five nodes were compared. Additionally, a container that exits
should be surfaced: Antithesis reports container exits, and a Cassandra process dying is worth
treating as a finding independent of any assertion.

### F4.4 — Two properties are specified in a way that will produce false positives the first time the workload gets aggressive

**Scope:** cross-cutting refinement. `b-replication-factor-never-under` and
`c-cms-membership-never-empty` both have a regime where the workload's own actions legitimately
violate the naive condition:

- Decommission enough nodes and RF cannot be satisfied — correct behaviour is *rejection*, but if
  the workload forces it, the assertion fires on an operator-caused state.
- Reconfigure the CMS to an RF the live node count cannot support — same shape.

`b-replication-factor-never-under.md` handles this well (the `min(RF, liveNodes)` floor, with the
two regimes named explicitly). `c-cms-membership-never-empty` does not — it has no equivalent
guard beyond `epoch >= FIRST`.

Worth stating why this matters more than it looks: the usual response to an assertion that fires
on correct behaviour is to weaken it until it stops. A P0 safety property weakened for noise is a
worse outcome than a slightly conservative property specified correctly from the start.

**Suggested action:** the workload should refuse to submit operations that would drop the ring
below RF or the CMS below its configured RF, and should log when it declines. That keeps the
assertions sharp and moves the constraint into the workload where it belongs. Declining to
submit is also more realistic — TCM is *supposed* to reject these, and `r-commit-rejected` already
covers the rejection path deliberately.

### F4.5 — The catalog has no property about the metadata log's *growth*

**Scope:** gap, low-to-moderate. The log is append-only and immutable. Snapshots exist to bound
replay cost. CEP-21 mentions "On-demand snapshots, truncation, and archival are also planned" —
planned, i.e. possibly not present.

A harness that commits metadata changes continuously for the whole run is a log-growth test
whether or not anyone intended it. If snapshot/truncation is incomplete, the observable failure is
gradual: catch-up gets slower, `LogState` transfers get larger, and eventually a node cannot catch
up within its timeout — which would surface as an `e-cluster-converges-after-faults` failure whose
root cause is unbounded log growth, easily misdiagnosed as a catch-up bug.

Not necessarily a property, but worth recording as run metadata: log length at the end of each
timeline, and whether snapshots are actually being taken. If log length grows linearly with no
snapshots, that is a finding regardless of what the assertions say.

## What is odd

**The `ProgressBarrier.immediate()` callsites.** `property-relationships.md` lists these as
Gap 5, and I want to underline why it is stranger than a gap. `immediate()` returns a barrier that
does not wait. Every use is a deliberate assertion by an author that *this* transition does not
need majority acknowledgement. That is a safety-relevant judgement made per-callsite, in code,
with no test that any of them is correct — and it is invisible to `b-progress-barrier-quorum-sound`
by construction, since that assertion lives in `await`.

An inventory of those callsites is cheap and would either produce confidence or produce a finding.
It is the highest ratio of insight to effort I found in this evaluation, and it is a code-reading
task rather than a testing task — which is probably why no lens is designed to catch it.

## Uncertainties

- Whether F4.1 is genuinely a gap in *this* work or correctly delegated to Harry and the in-tree
  simulator. `ci/harry_simulation.sh` and `test/simulator` exist; `9fe1a977b5` ("Get Harry working
  on top of Accord") and `0989a219ad` ("Fix HarrySimulatorTest.harryTest") show they are
  maintained. If Harry already covers data-path correctness under topology change, the
  control-plane-only scoping is defensible and F4.1 becomes an integration question rather than a
  coverage hole. I could not determine Harry's actual coverage from the files read.
- Whether Antithesis's own coverage instrumentation would surface F4.5 (log growth) as a
  utilization plateau in the triage report, making an explicit property unnecessary.
