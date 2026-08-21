# a-epoch-monotonic-per-node

## What led to this property

Commit `5d4bcc797a`, "Avoid exposing intermediate state while replaying log during startup"
(CASSANDRA-19384). The title is the property: during startup replay, a node was publishing
metadata states that were not its final state. A node that publishes epoch 300, then 150,
then 300 again has effectively un-applied and re-applied every change in between.

`Epoch` is a plain monotonic counter (`tcm/Epoch.java`), but it has several magic values
that make "monotonic" less trivial than it sounds:

```java
public static final Epoch FIRST = new Epoch(1);
public static final Epoch MAX = new Epoch(Long.MAX_VALUE);
public static final Epoch EMPTY = new Epoch(0);
public static final Epoch UPGRADE_STARTUP = new Epoch(Long.MIN_VALUE);
public static final Epoch UPGRADE_GOSSIP = new Epoch(Long.MIN_VALUE + 1);
```

`UPGRADE_STARTUP` and `UPGRADE_GOSSIP` are `Long.MIN_VALUE` and `MIN_VALUE + 1`, so they
compare as *less than* `EMPTY` (0) under `compareTo`, which is a plain `Long.compare` on
the raw value. A node transitioning out of an upgrade state therefore makes a legitimate
enormous jump upward, and any naive "epoch decreased" check must not treat the reverse —
a node somehow reporting `UPGRADE_*` after a real epoch — as anything other than a
violation.

## Code involved

- `tcm/log/LocalLog.java:555` — `committed.compareAndSet(prev, next)`, the single
  publication point. This is where the SUT-side assertion goes.
- `tcm/log/LocalLog.java:608` — `replayPersisted()`, the startup path CASSANDRA-19384 fixed.
  Note `replayComplete` gating at line 550: entries are not re-persisted during replay.
- `tcm/log/LocalLog.java:674,686` — `readyUnchecked()` / `ready()`, which decide when the
  node begins exposing metadata to the rest of the system.
- `tcm/Epoch.java:99-120` — `isDirectlyBefore`, `isDirectlyAfter`, `nextEpoch`, and the
  `beforeFirst` set (`EMPTY`, `UPGRADE_GOSSIP`, `UPGRADE_STARTUP`).

## What goes wrong if violated

A node that regresses its epoch revokes changes it has already acted on. Concretely: it
had applied a `DROP TABLE`, closed the `ColumnFamilyStore`, and answered a client; after
regression the table is back. Worse, because epoch is the divergence-detection token, a
regressed node advertises a *lower* epoch in messages and will be caught up by peers —
masking the regression as ordinary lag, so the only evidence is transient wrong answers.

## Expensive-to-rediscover details

- Two assertions, not one, and they catch different things. The SUT-side one at the CAS
  catches an in-process regression that a client might never observe (a fast down-then-up
  would be invisible to polling). The workload-side one catches a regression across a
  restart, which the SUT-side one cannot see because `prev` is reinitialised.
- The workload-side check must key its "highest seen" memory by **node identity**, not
  globally: nodes legitimately sit at different epochs, and a global high-water mark would
  fire on every lagging node.
- A container restart resets nothing from the workload's perspective — the node must come
  back at an epoch >= the one it last advertised, because the log is persisted. This makes
  the restart case the interesting one, and it is why the workload deliberately restarts
  nodes rather than relying only on Antithesis network faults.
