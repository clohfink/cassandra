# d-prepared-statement-not-stale

Added by evaluation gap G2 (`evaluation/coverage-balance.md` F2.2).

## What led to this property

Three fixed bugs on one seam, which is the strongest pattern in the whole history scan:

- `9bf1680b1f` "Avoid prepared statement invalidation race when committing schema changes" (CASSANDRA-20116)
- `1a6b8e0628` "Invalidate affected prepared stmts on every table metadata change"
- `740879d5a0` "Don't clear prepared statement cache on nodetool cms initialize"

Three commits about *when* prepared-statement invalidation runs relative to a schema commit. The
first is explicitly a race. The second widened the trigger from some changes to every change. The
third narrowed it back for one specific operation. That oscillation is characteristic of a seam
nobody is confident about.

## The seam, precisely located

From `d-schema-agreement-at-same-epoch.md`'s analysis of
`LocalLog.processPendingInternal` (lines 548–569), the ordering is:

```
storage.append(entry)              // 551 — persisted
notifyPreCommit(prev, next, ...)   // 553 — listeners, BEFORE publication
committed.compareAndSet(prev, next) // 555 — the epoch becomes visible
maybeNotifyListeners(entry, result) // 558
notifyPostCommit(prev, next, ...)   // 569 — listeners, AFTER publication
```

Anything maintained by a **post-commit** listener lags the published epoch by however long the
listener takes. Anything in **pre-commit** is visible atomically with it. Prepared-statement
invalidation is a listener; which hook it uses determines whether this property is a live
hypothesis or a cheap regression guard. That is the property's primary open question, and one code
read answers it for both this property and `d-schema-agreement-at-same-epoch`.

`TransactionalClusterMetadata.md` flags the general problem without resolving it:

> This entails some changes to the way the database objects represented in schema are intialised
> locally (db objects refers to classes like `Keyspace`, `ColumnFamilyStore`, etc).

## Why this property matters disproportionately

It is one of very few properties in the catalog that observes **client-visible** behaviour. The
evaluation's headline finding (`evaluation/synthesis.md` B1) is that 20 of 23 original properties
tested metadata as an object rather than the data-path guarantee metadata exists to provide. This
property does not close that bias — that needs a linearizability workload — but it is a genuine
instance of the missing category, obtained cheaply.

The failure it detects is a *wrong answer*, not an unavailability or an internal inconsistency: a
client receives rows shaped by a table definition the cluster has moved past.

## Code involved

- `tcm/log/LocalLog.java:553,569` — `notifyPreCommit` / `notifyPostCommit`.
- `tcm/listeners/` — the `ChangeListener` implementations.
- `tcm/transformations/AlterSchema.java` — the DDL transformation.
- `cql3/QueryProcessor` — the prepared-statement cache and its invalidation.
- `tcm/ClusterMetadata.java:111` — `DistributedSchema schema`, immutable and atomic with the epoch.

## How the workload constructs it

1. Create a probe table with a known column set.
2. Prepare a `SELECT` against it on a specific node's single-host session.
3. Insert a row.
4. `ALTER TABLE ... ADD` a new column, and write a known value into it.
5. Wait for that node's `describeCMS().EPOCH` to pass the alter's epoch.
6. Re-execute the prepared statement on the same session.

The assertion: the result must reflect the new definition, or the execution must fail cleanly
(unprepared / invalid). Returning the old shape *after the node's own epoch has advanced past the
change* is the violation.

Step 5 is what makes the property sound rather than racy — it does not assume any propagation
delay, it waits for the node to admit it has the change, then checks the derived state agrees.

## The driver re-preparation hazard

This is the detail most likely to silently defeat the property. The Java driver transparently
re-prepares on receiving an `unprepared` error and retries. If it does, a violation where the
server correctly rejects the stale statement would be invisible — the workload would see a
successful result with the new shape and conclude all is well, when what actually happened is the
server rejected it and the driver silently fixed it.

That is fine for the "fails cleanly" arm of the assertion (re-preparation *is* clean handling), but
it means the workload cannot distinguish "server served the new shape" from "server rejected, driver
re-prepared." Since both are acceptable outcomes, the property still holds — but the workload must
record which occurred, or the run cannot tell whether invalidation is working or whether the driver
is papering over a server that never invalidates.

Mitigation: track the driver's re-preparation events (or disable the behaviour if the driver
version allows) so the two acceptable outcomes are distinguishable in the report.

## What goes wrong if violated

A client executing a prepared statement gets rows shaped by a stale definition — missing a column
that exists, or worse, decoding bytes with the wrong type. Type-mismatched decoding is a silent
data corruption at the client, not an error. Because the node's epoch is current, no TCM
divergence-detection mechanism engages: the node is not behind, it just failed to propagate the
change into its own derived state.
