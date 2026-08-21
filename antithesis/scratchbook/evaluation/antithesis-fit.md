---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-14
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Guarantee statements evaluated for whether they need state-space exploration to falsify.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: Bug history used to judge whether each property targets a real timing/concurrency failure.
---

# Evaluation Lens 1 — Antithesis Fit

Does each property need state-space exploration that deterministic tests cannot reach, or is
it unit-test territory consuming search budget?

## Findings

### F1.1 — `a-metadata-identifier-unique`'s primary fault scenario is unreachable in this harness

**Scope:** property-specific. **Concern:** the property's stated Antithesis angle is "partition
the cluster into two halves during startup or during a CMS reconfiguration and let each half
attempt to establish or extend a CMS." But Antithesis does not inject faults until
`setup_complete`, and the harness emits `setup_complete` only after the cluster is healthy and
the CMS is initialized. **Startup therefore always happens on a clean network.** The
partition-during-initial-CMS-formation scenario cannot occur.

**Evidence:** the Antithesis setup guide states `setup_complete` is what "initiate[s] testing";
the test-command reference confirms `first_` commands run "after `setup_complete` but before any
other commands start" and that "No faults are injected during the execution of a `first`
command." The `deployment-topology.md` design has the workload entrypoint emit
`setup_complete` after a health check.

**Suggested action:** this is not fatal — the property remains reachable via a different route.
`wipe-and-restart` on the node control agent forces a node back through `Startup`/`Discovery`
*during* the fault-injected phase. The property's Antithesis angle should be rewritten around
that mechanism, and the workload must include a wipe-and-rejoin action. Without this the
property is a permanent no-op that looks like a pass.

### F1.2 — `d-ring-fully-owned` as stated is largely unit-testable

**Scope:** property-specific. **Concern:** "placements cover the ring with no gaps" is a pure
function of a `ClusterMetadata` value. A unit test over the placement calculation with
adversarial token sets would verify it more cheaply and more exhaustively than a distributed
run. Evaluated on a quiescent ring, the property consumes budget for no exploration value.

**Evidence:** the property's own implementation notes describe reconstructing ranges from a
sorted token list and checking contiguity — an operation on a single snapshot, with no
concurrency, timing, or partial-failure content.

**Suggested action:** keep the property but bind its value explicitly to the state it is
evaluated in. Its Antithesis-specific value is the *interrupted concurrent movement* case, so
the checker should record whether each evaluation occurred while `inProgressSequences` was
non-empty, and the run summary should report how many evaluations happened during churn versus
on a settled ring. A run whose evaluations were all on a settled ring tested nothing Antithesis
was needed for.

### F1.3 — Antithesis's value is underestimated for `d-peers-table-matches-directory`

**Scope:** property-specific. **Concern:** the property reads as routine derived-state
bookkeeping, but the bug history is unusually strong evidence of the opposite — Cassandra ships
a *repair tool* for this exact drift (`c484fc511a`). Drift that survives to need a repair tool
is drift that ordinary tests do not catch, which is the definition of good Antithesis territory.

The property currently scopes itself away from the most interesting window: it excludes nodes
with an active `multi_step_operation` and requires two-sample stability. That is a reasonable
hedge against not knowing the exact `NodeState` filter, but it means the check runs only on
settled state — precisely when drift is least likely to be observable *mid-transition*.

**Suggested action:** raise the property's expected value, and resolve the `NodeState` filter
open question so the mid-movement window can be checked rather than excluded. The exclusion is
currently a workaround for an unresolved question, and the evidence file says so honestly, but
the cost is higher than it looks.

### F1.4 — `r-coordinator-behind-rejection` may be a `Sometimes` that cannot fire

**Scope:** property-specific. **Concern:** the property needs three conditions to coincide — a
coordinator behind, intervening epochs that are *material* to the specific range or table, and a
request touching it. `TCM_implementation.md` is explicit that immaterial divergence does **not**
throw: the replica "will issue an asynchonous `TCM_FETCH_PEER_LOG_REQ` and attempt to catch up
from the peer" instead. The design deliberately narrows the throw to the material case.

Compounding this, the property's own investigation log concludes the exception is likely not
client-visible, so the observation mechanism is also unresolved. A `Sometimes` that cannot be
observed is indistinguishable in a report from a state that was never reached — which is worse
than having no property, because it reads as a tested-and-fine result.

**Suggested action:** resolve the observation mechanism before implementing
(`ExceptionsTable` is the leading candidate), and have the workload deliberately construct the
materiality condition rather than hoping for it: pin a connection to a node the workload has
just partitioned from the CMS, run DDL on the *specific* probe table that connection queries,
then query it. If after that it still does not fire, the honest conclusion is that the harness
cannot reach it, and the property should be marked as such rather than left silently green.

### F1.5 — Three properties are in Antithesis's strongest possible territory and should be
protected from dilution

**Scope:** catalog-wide, positive. `b-progress-barrier-quorum-sound`,
`c-cms-reconfiguration-quorum-overlap`, and `b-sequence-resumable-after-crash` each falsify a
*timing* claim that CEP-21 makes in prose and proves nowhere:

- "divergence cannot grow larger than a single epoch, so any two read or write quorums will
  have overlap"
- "Each step is only triggered once a majority of the participating nodes have acknowledged the
  preceding step"
- "We make *no assumptions* about liveness of the node between execution of in-progress
  sequence steps"

CEP-21 also records that a TLA+ spec of epoch visibility "was explored but omitted, since
maintaining it does not guarantee correctness of the final product," and that the quorum
intersection argument rests on a simulator whose results are claimed "exhaustive." So these
three claims currently have no mechanical check at all. That is the highest-value thing in the
catalog.

**Suggested action:** none needed, but their guards (`r-progress-barrier-relaxed`,
`r-cms-reconfiguration-observed`) must be treated as release blockers for the harness itself. If
those two never fire, the harness's most valuable properties are inert.

## Passes

- Every Category A and B safety property targets a documented partial-failure or concurrency
  bug from the git history, not an input-validation concern.
- No property is a pure input-validation or serialization check. (`60fe2dc61d`, a serialization
  version bug, was correctly *not* turned into a property — that is unit-test territory.)
- Assertion types match testing mode throughout: the two `AlwaysOrUnreachable` uses are both on
  genuinely optional paths, and no `Sometimes(true)` smell appears.
- The six reachability properties are proportionate — roughly one guard per safety cluster,
  which is what makes green results interpretable.

## Uncertainties

- Whether thread pausing will actually reach the `LocalLog.processPendingInternal` CAS window
  frequently enough to matter for `a-epoch-monotonic-per-node`. This depends on instrumentation
  granularity inside a JVM, which the coverage-instrumentation docs describe as bytecode weaving
  but do not characterise in terms of pause-point density. Unresolvable from documentation.
- Whether five Cassandra JVMs leave enough headroom for meaningful exploration per timeline, or
  whether the harness will be so slow that few interleavings are explored regardless of property
  quality. Measurable locally; noted in `deployment-topology.md`.
