# r-concurrent-multistep-operations

## What led to this property

Concurrent disjoint-range movements are the headline capability TCM added over gossip. CEP-21
frames the whole goal as supporting "multiple concurrent additions, removals and replacements
without compromising durability, correctness or availability," permitted "as long as they only
affect disjoint token ranges."

`b-no-overlapping-locked-ranges` tests the mechanism that makes that safe. But that property
is only meaningful if two operations are ever actually in flight at once — and with a small
cluster, a naive workload will serialise them without meaning to, because each operation takes
a long time and the obvious implementation waits for one to finish before starting the next.

This is the guard against a green report that means nothing.

## What it guards

Directly: `b-no-overlapping-locked-ranges`, `b-locked-ranges-match-sequences`. Indirectly
`b-replication-factor-never-under` and `d-ring-fully-owned`, whose interesting cases are
concurrent movements on adjacent ranges.

If this `Sometimes` never fires, all-green on those four is uninformative — not wrong, just
uninformative, which is worse because it looks like evidence.

## Code involved

- `tcm/sequences/InProgressSequences.java` — the set whose size is the condition.
- `db/virtual/ClusterMetadataDirectoryTable.java` — the `multi_step_operation` column
  (`MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false)`), populated per node
  from `ClusterMetadata.current()`.
- `CMSOperationsMBean.dumpDirectory(boolean includeTokens)` — the JMX equivalent.
- `StorageServiceMBean.joinRing()`, `decommission()`, `move(String newToken)` — the workload's
  triggers.

## Why `Sometimes` and not `Reachable`

`Reachable` marks that a line executed. What matters here is a *state*: two sequences
overlapping in time. There is no single line whose execution implies it. The condition
`count(inProgressSequences) >= 2` is exactly the semantic state, which is what the docs
describe `Sometimes` as being for — "non-trivial semantic states that should become true at
least once during a run."

## How the workload actually achieves it

Not by luck. Three deliberate choices:

1. **Fire-and-forget triggers.** `joinRing()` and `decommission()` are invoked without waiting
   for completion, so the workload can start a second operation while the first is mid-sequence.
2. **Two spare nodes, not one.** With a single spare there is at most one join in flight; the
   topology carries two specifically so a join and a decommission — or two joins — can overlap.
   This is why `deployment-topology.md` justifies the fifth Cassandra container.
3. **Disjoint-range bias.** Token assignment for the spares places them on opposite sides of
   the ring, so concurrent operations are more likely to be *admitted* rather than rejected for
   overlap. Rejections are also wanted (see `r-commit-rejected`), but if every concurrent
   attempt is rejected, this property never fires.

Point 3 is the subtle one: this property and `r-commit-rejected` pull in opposite directions,
and the workload needs both to fire, so it must sometimes choose overlapping ranges and
sometimes disjoint ones rather than always one or the other.

## Implementation notes

- Evaluate in the periodic invariant checker (`anytime_` command), which is already sampling
  the directory for other properties, so this costs nothing extra.
- Count distinct sequence keys across all nodes, taking the maximum over nodes rather than
  reading one node — a node behind by an epoch may see only one sequence.
- Report the observed maximum concurrency in the run summary even when the assertion passes.
  "Fired at least once" is the pass bar, but "peaked at 2 out of a possible 3" is the number
  that tells you whether to make the workload more aggressive.

## First Antithesis run (2026-08-18, run 8bf9b2c6...-59-13) — NEVER REACHED

`ex=0` across the whole 30-minute run: the workload never had two multi-step operations in flight
simultaneously, even under fault injection. This is the most important negative from the run,
because it means the concurrency-admission safety properties (`b-no-overlapping-locked-ranges`,
`b-locked-ranges-match-sequences`) were **not meaningfully exercised** — an all-green on those is
currently vacuous.

Why it did not fire: the workload issues one membership operation per `serial_driver_membership_churn`
invocation, fire-and-forget, but the ring hovered at exactly RF (3), so the R9 decline-below-RF
guard blocked most decommissions/moves, and joins were gated on spares being available. Two
operations rarely overlapped in time.

To fix (workload work, `antithesis-workload`): drive both spares to `joinRing()` back-to-back
without waiting; or run an explicit "start move on node A and join on spare B" pair in one command;
or raise the ring size so decommission+join can overlap while staying above RF. The goal is to make
`inProgressSequences` reach size >= 2 regularly, then confirm this property flips to satisfied.

## Fix (2026-08-18, after run 8bf9b2c6...-59-13)

Three root causes, all fixed:

1. **Blocking JMX calls serialised the operations.** `StorageServiceMBean.joinRing()` is
   `synchronized` and blocks until bootstrap completes; `move()`/`decommission()` block too. The old
   `membershipChurn` issued one op per invocation and blocked on it, so operations never overlapped.
   Fix: a dedicated `serial_driver_concurrent_movements` command (`Actions.launchConcurrentMovements`)
   launches each operation on its own daemon thread, so the blocking calls run at once.
2. **`move` is unsupported with vnodes.** `num_tokens=4`, and `nodetool move` throws "this node has
   more than one token and cannot be moved thusly". Removed the move path (from the concurrency
   driver and from `membershipChurn`); concurrency now comes from concurrent **joins**.
3. **Deterministic token allocation rejected the second join.** `conf/cassandra.yaml` ships
   `allocate_tokens_for_local_replication_factor: 3`, so two spares bootstrapping at once computed
   identical tokens and TCM rejected the second ("some tokens are already assigned"). The entrypoint
   now comments it out → random allocation → two simultaneous joins get disjoint tokens.

**Local verification (no faults):** two concurrent joins are now BOTH admitted — the ring grows
3→5 with no rejection. The assertion still saw `peak_in_flight=1` locally, because on a healthy
cluster progress barriers pass instantly and each bootstrap finishes faster than the sampler; and
two concurrent *decommissions* serialise on overlapping vnode ranges in such a small ring. Under
Antithesis, `progress_barrier_timeout` is 60s and injected partitions stall barriers, so admitted
joins persist for seconds-to-minutes and the overlap is readily observable. The assertion
(`Assert.sometimes`, message owned by `launchConcurrentMovements`) samples in a tight loop for 30s
after launching. The dedicated command is the single owner of this message; `Checks.concurrentSequences`
no longer asserts (it only records the peak for context).

## Second fix (2026-08-19, after run 32d96d63...-59-13 still showed ex=0)

Run 4's concurrency counterexample was decisive: `launched=[join:cassandra-4, join:cassandra-5],
spares=2, peak_in_flight=1` -- the ideal case (two joins launched with two spares) still reported
peak 1. Root cause: the in-flight counter collected distinct `multi_step_operation` *strings* into a
`Set<String>`. Two bootstraps at the same phase render an identical mso map, so the set collapsed
two genuinely-distinct sequences (owned by two different nodes) to one. Fixed in both
`Actions.currentInProgressSequences` and `Checks.concurrentSequences`: count directory entries that
carry an mso (each keyed by a distinct owning node id), not distinct mso strings.

Local verification remains inconclusive by nature: with no fault injection and an empty probe
keyspace, every multi-step operation (join, decommission) completes in about a second, so two never
coexist in the directory long enough to sample -- confirmed by watching two concurrent decommissions
both reach LEFT almost immediately. Under Antithesis, `progress_barrier_timeout=60s` and injected
partitions stall the sequences for seconds-to-minutes, which is exactly when the (now-correct)
counter will report >= 2. If the next run STILL shows ex=0, the open question narrows to "were two
sequences ever simultaneously in `inProgressSequences`" -- answerable by archaeology on the run's
directory dumps -- rather than a counting artifact.
