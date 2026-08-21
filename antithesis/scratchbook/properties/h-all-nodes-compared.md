# h-all-nodes-compared

Added by evaluation gap G5 (`evaluation/wildcard.md` F4.3). This is a property about the
**harness**, not the SUT.

## What led to this property

A structural weakness that cross-cuts every lens, raised as an open question in
`deployment-topology.md` and escalated by the wildcard lens.

At least six properties are evaluated only over "nodes that answered":
`a-log-prefix-agreement`, `a-metadata-identifier-unique`, `c-cms-membership-never-empty`,
`c-initialization-abort-recoverable`, `d-schema-agreement-at-same-epoch`, and
`e-cluster-converges-after-faults`. That scoping is necessary — a node behind a partition
contributes no evidence, and treating silence as either agreement or disagreement would be wrong.

But it creates a failure mode with no signal:

- Node unreachable because Antithesis partitioned it → excluding it is **correct**.
- Node unreachable because its process died → excluding it **hides the failure the harness exists
  to find**.

From the workload, both are a timeout. Identical.

## Why this needs to be an assertion rather than a comment

Because the degradation is invisible in the report. A run where two of five nodes were never
included in any cross-node comparison produces exactly the same green output as a run that
compared all five every cycle. There is no field in a triage report that says "your checks only
looked at three nodes."

The specific predicted case that makes this concrete: `deployment-topology.md` and
`evaluation/implementability.md` both flag an unresolved question about whether `CMSOperations`
registers its MBean on a node started with `-Dcassandra.join_ring=false`. If it does not, both
spare nodes are permanently unobservable, and every cross-node property silently drops 2 of 5
nodes for the entire life of the harness — while reporting full green. This property turns that
into a visible failure on the first run.

## Why `Sometimes` is the right type

Full participation cannot be `Always`: partitions legitimately prevent it, and that is the point
of the faults. But it must happen *sometimes*, or the cross-node properties were never actually
evaluated at full strength.

`Sometimes(evaluatedNodeCount == totalNodeCount)` states exactly that. It is a meaningful semantic
condition ("a comparison had complete information"), not a line-reached marker, so `Sometimes`
rather than `Reachable`.

## Implementation notes

- Every cross-node checker returns its evaluated-node count alongside its verdict. The `anytime_`
  checker aggregates and evaluates this assertion once per cycle.
- `totalNodeCount` must be a **configured constant** (5), not the count of nodes the workload
  currently knows about. Deriving it from discovery would make the property self-satisfying: if the
  workload can only see three nodes, `3 == 3` and it passes.
- Record the distribution of evaluated counts across the run as metadata. "Reached 5/5 once" is the
  pass bar; "reached 5/5 in 2% of cycles" is a finding about the harness's fault profile even
  though the assertion passes.
- Also worth surfacing separately: container exits. Antithesis reports these, and a Cassandra
  process dying is a finding independent of any assertion — but it is reported through a different
  channel than assertion outcomes, so it needs to be looked at deliberately rather than assumed to
  show up alongside property results.

## What this property cannot do

It detects that comparisons were incomplete. It does not detect *why*, and it cannot recover the
evidence that was missed. A run where node 3 died early and this property fired tells you the
cross-node results are untrustworthy; it does not tell you what node 3 would have shown.

That is acceptable — the value is in never mistaking a degraded run for a clean one. But it means
this property is a tripwire, not a diagnostic, and a firing should trigger investigation of
container exits and agent `status` output rather than being read as a finding in itself.

## Precedent for harness self-checks

This is the only Category H property, and the category exists because the concern is real enough to
name: a test harness that silently stops testing is worse than one that fails loudly, because it
manufactures unearned confidence. The Antithesis sizing documentation makes an adjacent point about
diagnosing weak setups — that a utilization graph hitting "a hard horizontal asymptote" means
"additional test cases aren't provoking any new behavior or code paths," and that the cause could
be "misconfiguration, a weak workload, or not enough parallelism." Distinguishing those is exactly
what harness-level instrumentation is for. This property is the same instinct applied to check
coverage rather than code coverage.
