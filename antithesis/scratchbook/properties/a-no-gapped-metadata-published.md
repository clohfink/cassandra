# a-no-gapped-metadata-published

## What led to this property

Two things converged:

1. Commit `44ee9d6167`, "Unable to catch up TCM Log from peer with gaps in log sequence"
   (CASSANDRA-21455) — recent, and exactly this area. Plus `693eab8776`, which *reverted*
   "changes to serving FetchCMSLog/FetchPeerLog requests & remove ReconstructLogState",
   meaning an earlier attempt at this code was backed out. Reverted work in a correctness
   path is a strong signal.
2. The maintainers already wrote the invariant, as a Java `assert`, at
   `tcm/log/LocalLog.java:544`.

## Code involved

The whole property lives in `LocalLog.processPendingInternal()` (lines 492–603). The
guard is:

```java
if (pendingEntry.epoch.isDirectlyAfter(prev.epoch)
    || ((isPreInit || isSnapshot) && pendingEntry.epoch.isAfter(prev.epoch)))
```

and the post-condition asserts are:

```java
assert pendingEntry.epoch.is(next.epoch);                                  // line 542
assert next.epoch.isDirectlyAfter(prev.epoch) || isSnapshot
       || pendingEntry.transform.kind() == Transformation.Kind.PRE_INITIALIZE_CMS;  // line 544
```

## The comment that makes this worth attacking

Lines 513–517, verbatim:

> only a PRE_INITIALIZE_CMS or a snapshot is allowed to skip over gaps.
> Note: INITIALIZE_CMS is not allowed to do this as during upgrades it can allow us to jump
> over the PRE_INITIALIZE_CMS entry if the INITIALIZE_CMS is received first. This then
> creates a gap at Epoch.FIRST which can never be resolved. In turn, that makes it
> impossible to build a LogState for replay purposes with a correct and consecutive set of
> entries if the node is bounced before applying a later snapshot.

This is a guard shaped around one specific discovered failure — a permanent, unresolvable
gap at `Epoch.FIRST` — expressed as a three-way kind check. Guards of that shape usually
have siblings: the question is whether some *other* entry kind, or some other arrival
order, produces the same unresolvable gap.

## Interacting mechanisms

- The `pending` map's comparator (line 242) deliberately orders `FORCE_SNAPSHOT` ahead of
  everything else, so a snapshot always gets first look. After it applies, entries below
  its epoch are dropped.
- The `else if (!pendingEntry.epoch.isAfter(metadata().epoch))` branch (586) discards
  already-applied entries.
- The final `else` (592) returns when the smallest pending entry is non-consecutive —
  "if this one was not consecutive, subsequent won't be either". Correct given the
  comparator, and load-bearing on it.
- `hasGaps()` and `highestPending()` exist as public API, which tells us gaps are an
  expected transient state, not an error state.

## What goes wrong if violated

Publishing metadata from a gapped log means skipping a transformation. If the skipped
entry was `PrepareLeave` for node X, this node believes X still owns its ranges while
every other node has moved on. It routes reads to X and, being at a *higher* epoch than
the entry it skipped, has no mechanism that will ever tell it otherwise.

## Notes for implementation

- `Assert.always` at both sites, with the existing `assert`s left in place. Cassandra's
  test JVMs run with `-ea`, so removing them would weaken unit and dtest coverage; the
  Antithesis assertion adds always-on reporting and search guidance.
- The `Details` payload should carry `prev.epoch`, `next.epoch`, and `kind` — on a
  triage-report failure those three values immediately distinguish "illegal jump" from
  "wrong kind allowed to jump."
- `throw new StopProcessingException(t)` (532/538) means a transform failure halts
  processing entirely. A node that stops processing is stuck at its current epoch forever,
  which will surface as an `e-cluster-converges-after-faults` failure rather than here —
  worth knowing when triaging, since the root cause is in this method either way.

## Investigation Log

#### Does the single-caller contract on `processPendingInternal` hold across all arrival paths?

- Examined: the method's own doc comment (lines 479–491), the `Async`/`Sync` subclasses
  (719–938), `append(Entry)`, `append(Collection<Entry>)`, `append(LogState)`,
  `maybeAppend`, and the abstract `processPending()` declaration at line 471.
- Found: the contract is stated as a *requirement on implementations* — "Implementations
  have to guarantee there can be no more than one caller of `processPendingInternal()` at
  a time, as we are making calls to pre- and post- commit hooks. In other words, this
  method should be called _exclusively_ from the implementation, outside of it there's no
  way to ensure mutual exclusion without additional guards." The `Async` variant serialises
  via a single `Interruptible` runnable. The `IllegalStateException` at 565 is the tripwire
  for a violation, and its message includes both epochs.
- Not found: whether `Sync.processPending()` can be entered re-entrantly from a listener
  callback, and whether startup replay can overlap the async runnable's first tick.
- Conclusion: tagged `(needs human input)`. A maintainer can answer in one sentence; the
  experiment (looking for the `IllegalStateException` in triage logs) is a weaker and
  slower substitute. Meanwhile the harness treats that `IllegalStateException` appearing
  in any container log as a finding worth escalating, independent of assertion outcomes.
