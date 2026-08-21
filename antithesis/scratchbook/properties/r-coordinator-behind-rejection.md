# r-coordinator-behind-rejection

## What led to this property

`CoordinatorBehindException` is the request-path half of TCM's consistency story. Every other
property in this catalog is about the control plane; this one is about whether the control
plane's state actually affects reads and writes correctly.

From `TCM_implementation.md`:

> Replicas can check the schema and ring consistency of the *current* request by comparing the
> `Epoch` that coordinator has with the epoch when schema was last modified, and when the
> placements for the given range were last modified. If it happens that the replica knows that
> coordinator couldn't have known about either schema, or the ring, it will throw
> `CoordinatorBehindException`.

The design is deliberately narrow, and the narrowness is what makes reachability uncertain: the
exception fires **only** when the divergence is *material* to the request. If the coordinator is
behind but the intervening epochs did not change this range's placements or this table's schema,
the replica does not reject — it asynchronously catches up via `TCM_FETCH_PEER_LOG_REQ` and
serves the request. So reaching it needs a coordinator behind by epochs that specifically
mattered.

## Why it belongs in the catalog

Without it, the harness tests the metadata log in isolation. TCM's purpose is not a correct log
for its own sake — it is correct reads and writes. This property is the minimum evidence that
the two are connected: that metadata divergence is *detected* at request time rather than
merely minimised.

CEP-21 sets the bar higher than detection, in the coordinator-side check:

> After coordinator has collected enough responses, it compares its `Epoch` with the `Epoch`
> that was used to construct the `ReplicaPlan` for the query it is coordinating. If epochs are
> different, it checks if collected replica responses still correspond to the consistency level
> query was executed at.

A fuller treatment would assert that check's outcome too. That is a larger piece of work — it
needs a linearizability-style workload with a history checker rather than a metadata workload —
and is recorded in `property-relationships.md` as the main gap in the catalog's coverage.

## Code involved

- `exceptions/CoordinatorBehindException.java` (referenced from `TCM_implementation.md`).
- The read/write verb handlers that compare the request epoch against
  `lastModified` on placements and schema.
- `tcm/FetchPeerLog.java`, `tcm/PeerLogFetcher.java` — the non-rejecting catch-up path taken
  when divergence is immaterial.
- `tcm/Epoch.java` — the comparison primitives.
- `e182744cd0` "Introduce 5.1 messaging format that brings in Epoch" — the messaging change
  that makes per-request epoch comparison possible at all.

## How the workload reaches it

The three conditions must coincide:

1. A coordinator serving client traffic while behind — achieved by partitioning one node from
   the CMS while continuing to send it CQL through the workload's per-node connection.
2. Intervening epochs that *matter* — the workload runs DDL on the probe table and range
   movements affecting the probe keyspace, not unrelated changes.
3. A request that touches the affected range or table.

The workload therefore keeps a dedicated connection per node with the driver's token-aware and
load-balancing policies pinned to that node, so it can deliberately coordinate through a node it
knows to be lagging. A default driver configuration would route around the lagging node and this
property would never fire.

## Implementation notes

- Workload-side `Sometimes`. The condition is set when the workload observes the error from a
  request it made.
- The observation mechanism depends on the open question below. If the exception is not
  client-visible as a distinguishable error, the fallback is to scan container logs for the
  exception class name, or read an `ExceptionsTable` row — `db/virtual/ExceptionsTable.java` is
  registered in `SystemViewsKeyspace` and would surface server-side exceptions without needing a
  log scrape.

## Investigation Log

#### Is `CoordinatorBehindException` surfaced to a CQL client as a distinguishable error?

- Examined: `TCM_implementation.md`'s Querying section, which names the exception and describes
  the replica-side throw and the coordinator-side re-check; `tcm/Epoch.java` for the comparison
  API; `db/virtual/SystemViewsKeyspace.java` registrations, which include
  `new ExceptionsTable(VIRTUAL_VIEWS)`.
- Found: the exception is thrown by a *replica* while handling an internode message, not by the
  coordinator while handling the client request. That strongly suggests it is handled internally —
  the natural coordinator behaviour on a replica error is to catch up and retry, not to propagate
  a server-internal condition to the client. `ExceptionsTable` exists as a server-side
  observation channel, which is the more likely route.
- Not found: the exception's `ExceptionCode` / whether it maps to a driver-visible error type,
  and whether the coordinator retries transparently.
- Conclusion: tagged `(partial)`. This changes the implementation but not the property. The
  `ExceptionsTable` route is the leading candidate and needs no new SUT surface; a metric would
  be better still if one exists. Worth resolving before implementing the checker, since guessing
  wrong means a `Sometimes` that can never fire — which is indistinguishable in the report from
  a workload that never reached the state, and therefore actively misleading.

## First Antithesis run (2026-08-18, run 8bf9b2c6...-59-13) — NEVER REACHED

`ex=0` across the run: `CoordinatorBehindException` was never observed. This confirms the open
question above — either the chosen observation mechanism (`system_views.exceptions`) does not
surface it, or the materiality condition (a coordinator behind by epochs that matter to the exact
range/table it queries) was never constructed under the run's fault profile. Both are plausible and
this run does not distinguish them.

Next step (workload work): first confirm observability out-of-band — provoke the condition
deterministically in a local dtest or by partitioning one node from the CMS while running DDL on the
exact table its pinned session reads, and check whether anything appears in `system_views.exceptions`
or a metric. If nothing is observable there, the property needs a different signal (a driver-visible
error type, or a server-side metric/log scrape) before it can ever pass.

## Fix (2026-08-18, after run 8bf9b2c6...-59-13)

The observation was fundamentally wrong and is now corrected. Source inspection settled the open
question: `CoordinatorBehindException` is **never client-visible** — `StorageProxy` catches it on the
coordinator and *retries* the request (marking `ClientRequestMetrics.RetryCoordinatorBehind`), so the
client sees success and `system_views.exceptions` never captures it. The reliable signal is the
**replica-side JMX meters**, marked immediately before each throw in `ReadCommandVerbHandler` /
`AbstractMutationVerbHandler` / `ReadCommand` / `PartitionUpdate`:

- `org.apache.cassandra.metrics:type=TCM,name=CoordinatorBehindSchema`
- `org.apache.cassandra.metrics:type=TCM,name=CoordinatorBehindPlacements`

`coordinatorBehindProbe` now (a) triggers by churning the probe table's schema and driving a
write+read through every node's pinned single-host session, so a lagging node coordinates a
materially-relevant request, and (b) observes by reading those meters' cumulative `Count` across all
nodes; nonzero anywhere ⇒ the condition occurred. Local (no-fault) verification: the meter-read path
reads all 5 nodes cleanly and reports 0 (correct — no coordinator is behind without partitions).
Under Antithesis faults it should register. If it still shows ex=0 after the next run, the remaining
question is purely whether the fault profile creates a *materially-relevant* lag, not observability.
