# b-no-overlapping-locked-ranges

## What led to this property

CEP-21 makes non-overlap the sole admission condition for concurrent topology change:
concurrent range movements "may only be permitted where there is no overlap between the
affected ranges," and "Concurrent operations are permitted as long as they only affect
disjoint token ranges, ensuring that concurrent range movements remain safe and cluster
invariants are preserved at all times."

That is a big claim resting on one predicate. `LockedRanges.intersects()` is the entire
enforcement mechanism for TCM's headline capability over gossip.

## Code involved

`tcm/sequences/LockedRanges.java`:

```java
public static final LockedRanges EMPTY = new LockedRanges(Epoch.EMPTY, ImmutableMap.of());
public static final Key NOT_LOCKED = new Key(Epoch.EMPTY);
public final ImmutableMap<Key, AffectedRanges> locked;

public LockedRanges lock(Key key, AffectedRanges ranges)   // line 67
public LockedRanges unlock(Key key)                        // line 82
public Key intersects(AffectedRanges ranges)               // line 93 — returns NOT_LOCKED if clear
```

`intersects` iterates the map and returns the first colliding key:

```java
if (ranges.intersects(e.getValue()))
    return e.getKey();
```

Keys are epoch-derived (`keyFor(Epoch)`), so a lock is identified by the epoch of the
`Prepare*` that created it. `AffectedRanges` is per-`ReplicationParams` — a
`Map<ReplicationParams, Set<Range<Token>>>` — so "overlap" is evaluated per replication
setting, not globally over the ring. Two keyspaces with different RF can have locks on the
same token range without colliding, which is correct but is also the kind of dimension
where an implementation can accidentally compare the wrong pair.

Admission callers: `tcm/transformations/PrepareJoin.java`, `PrepareLeave.java`,
`PrepareMove.java`, `PrepareReplace.java`. TCM_implementation.md, on `PrepareJoin`:
"If computed locked ranges intersect with ranges that were locked before this transformation
got executed, `PrepareJoin` is rejected."

## The race that matters

`Prepare*` validation and log append are not one atomic step from the submitter's point of
view. The CMS "verifies that the proposed change would be valid if all previously accepted
and committed changes have been applied," and on a lost Paxos race, CEP-21 says the
proposal is retried "if still valid" — otherwise rejected. So the window is:

1. Operation A computes affected ranges against metadata at epoch N.
2. Operation B commits at N+1, locking overlapping ranges.
3. A loses its Paxos round, retries, and is re-validated.

Step 3 must re-run `intersects` against N+1, not reuse the N verdict. If any path caches
the computed `AffectedRanges` alongside a stale validity decision, two overlapping locks
land. Antithesis reaching this requires two concurrent prepares plus a lost Paxos round —
a combination that is essentially unreachable by scripted testing but routine under
injected partitions.

## What goes wrong if violated

Two sequences independently recompute `DataPlacements` for the same range from different
base states. The later write wins and silently discards the other's replica set changes.
The visible outcome is a range whose replica set matches neither operation's plan, with no
error logged anywhere — under-replication that only surfaces when a node in the
incorrectly-computed set is lost.

## Implementation notes

- `Assert.always` inside `lock()`, evaluating `intersects(ranges)` on the *pre-existing*
  map before constructing the new one. Placing it in `lock()` rather than in each
  `Prepare*` gives one callsite covering all four operations, and catches any future
  caller that forgets to check.
- Strict form, no same-key exemption — see the resolved investigation below. The
  key-excluding variant (`intersects(ranges) == NOT_LOCKED || intersects(ranges).equals(key)`)
  turned out to be unnecessary and would have weakened the property.
- The `Details` payload should include the colliding key and the intersecting range set;
  without them a triage report says only "two locks overlapped."

## Investigation Log

#### Is `lock()` ever legitimately called with an overlapping key during sequence advance?

**RESOLVED** during implementation (2026-08-14). Reading `lock()`'s body rather than just its
signature settles it: the new map is built with

```java
ImmutableMap.<Key, AffectedRanges>builderWithExpectedSize(locked.size())
            .putAll(locked)
            .put(key, ranges)
            .build();
```

Guava's `ImmutableMap.Builder.build()` **throws** `IllegalArgumentException` on a duplicate key (it
is `buildKeepingLast()` that tolerates one). So `lock()` can never legitimately be called with a key
already present in `locked` — it would already fail today, loudly. Any intersection `intersects()`
finds at that point therefore belongs to a *different* operation, which is exactly the violation.

Consequence: the assertion uses the **strict** form, `intersects(ranges).equals(NOT_LOCKED)`, with no
same-key exemption. The weaker key-excluding form drafted below is unnecessary, and using it would
have silently weakened a P0 property. The in-tree `TODO might we need the ability for the holder of a
key to lock multiple sets over time?` immediately above the builder confirms the design intent:
locking happens once per key.

Original investigation, retained as audit trail:

- Examined: `LockedRanges.java` in full at the API level — `lock`, `unlock`, `intersects`,
  `keyFor`, `NOT_LOCKED`, `EMPTY`, the `AffectedRanges`/`AffectedRangesBuilder` interfaces
  and `AffectedRangesImpl`; `MultiStepOperation.java`'s `advance(CONTEXT)` and
  `cancel(ClusterMetadata)` signatures.
- Found: `lock(key, ranges)` writes into an `ImmutableMap` by key, so calling it twice with
  the same key replaces rather than duplicates. That makes a same-key re-lock harmless in
  effect but indistinguishable from a violation by a naive `intersects != NOT_LOCKED` test —
  the map still contains the old entry when the check runs. `MultiStepOperation.advance`
  returns a new operation with a new `latestModification` epoch, and `LockedRanges` has its
  own `withLastModified(Epoch)`, so lock state is re-stamped as sequences progress.
- Not found: whether `advance` implementations in `BootstrapAndJoin` / `UnbootstrapAndLeave`
  / `Move` / `BootstrapAndReplace` call `lock()` again per step, or lock once at prepare and
  only `unlock()` at completion.
- Conclusion: tagged `(partial)`. The safe implementation is the key-excluding form above,
  which is correct under either answer. Resolving it would let the assertion be strictly
  stronger, so it is worth reading the four `advance` bodies before finalising — but it does
  not block a first run.
