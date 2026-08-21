# a-log-processing-never-concurrent

Added by evaluation gap G3 (`evaluation/coverage-balance.md` F2.3).

## What led to this property

Two observations that only connect when you look at them together.

First, `LocalLog.processPendingInternal` declares its own mutual-exclusion requirement as a
*contract on callers* rather than enforcing it (lines 479–491):

> Implementations have to guarantee there can be no more than one caller of
> `processPendingInternal()` at a time, as we are making calls to pre- and post- commit hooks. In
> other words, this method should be called _exclusively_ from the implementation, outside of it
> there's no way to ensure mutual exclusion without additional guards.

Second, there is already a tripwire for the contract being broken, at line 555–567:

```java
if (committed.compareAndSet(prev, next))
{
    logger.info("Enacted {}. New tail is {}", pendingEntry.transform, next.epoch);
    maybeNotifyListeners(pendingEntry, transformed);
}
else
{
    // Since we disallow concurrent calls to `processPendingInternal` (as declared in the interface),
    // we might have made an erroneous extra initialization of keyspaces by now, and, unless we
    // throw here, we may in addition call to `afterCommit`.
    throw new IllegalStateException(String.format("CAS conflict while trying to commit entry with seq %s, old version tail: %s current version tail: %s",
                                                  next.epoch, prev.epoch, metadata().epoch));
}
```

The comment is unambiguous about what reaching the `else` means, and about the consequence: an
"erroneous extra initialization of keyspaces" has already happened by then.

## Why the tripwire is not enough today

The throw is inside the `try` block whose `catch (Throwable t)` sits at line 575:

```java
catch (Throwable t)
{
    JVMStabilityInspector.inspectThrowable(t);
    logger.error("Could not process the entry", t);
}
```

`IllegalStateException` is not `StopProcessingException`, so it is caught here, logged, and
processing continues. The `finally` at 580 then removes the entry from `pending`.

So the current behaviour on a violated core invariant is: a log line, a dropped entry, and a node
that carries on with keyspaces it may have double-initialized. Nothing fails. No test asserts on
it. `Assert.unreachable` turns that into a reported property without changing control flow —
which matters because the SDK's assertions do not terminate the program, so adding one here
cannot make the existing (bad) recovery behaviour worse.

## The five arrival paths

The contract is about callers, so the hypothesis is about which callers exist. Entries reach
`pending` from:

1. `Replicator` broadcast after a CMS commit (best-effort, no retries)
2. `TCM_FETCH_PEER_LOG_REQ` responses via `PeerLogFetcher`
3. `TCM_FETCH_CMS_LOG_REQ` responses
4. Startup replay — `replayPersisted()` at line 608
5. Synthetic `ForceSnapshot` insertion at the head of the buffer

`LocalLog` has `Async` (line 719) and `Sync` (910) variants. `Async` serialises through a single
`Interruptible` runnable (`AsyncRunnable.run`, line 827), which is a real mechanism. `Sync`'s
`processPending()` is called by whoever appends — and `awaitAtLeast` (925) is also on the `Sync`
path. Whether a listener callback can re-enter, or whether startup replay can overlap the async
runnable's first tick, is the open question.

## What goes wrong if violated

`notifyPreCommit(prev, next, ...)` runs before the CAS. Its listeners initialise database objects
— `Keyspace`, `ColumnFamilyStore` — for the new metadata. If two threads both get through
pre-commit and only one wins the CAS, the loser has already initialised objects for metadata that
was never published. The comment's phrase "erroneous extra initialization of keyspaces" is
describing exactly this. Downstream effects would be arbitrary: duplicate CFS instances,
listeners fired for an epoch that does not exist, prepared statements invalidated against a
phantom schema.

## Implementation notes

- `Assert.unreachable` in the `else` branch, *before* the `throw`, so the assertion fires whether
  or not the exception is later swallowed.
- `Details` must carry `next.epoch`, `prev.epoch`, `metadata().epoch`, and
  `pendingEntry.transform.kind()`. The three epochs are already in the exception message; the kind
  is what identifies the arrival path, which is what a fix would need.
- Thread pausing is the fault that reaches this, and thread pausing requires coverage
  instrumentation — so this property is a concrete reason the jars must be in
  `/opt/antithesis/catalog/` rather than only cataloged for assertions.

## Relationship to other properties

Resolves, empirically, the `(needs human input)` open question in
`a-no-gapped-metadata-published.md` — "Does the single-caller contract hold across all five
entry-arrival paths?" That question was tagged needs-human-input because reading the code could
not settle it. An `Unreachable` assertion settles it from the other direction: if it never fires
across many timelines with thread pausing active, that is meaningful evidence; if it fires, the
question is answered definitively and with a reproducible trace.
