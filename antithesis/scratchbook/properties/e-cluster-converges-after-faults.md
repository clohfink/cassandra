# e-cluster-converges-after-faults

## What led to this property

The failure mode CASSANDRA-21455 produced. Commit `44ee9d6167`, "Unable to catch up TCM Log
from peer with gaps in log sequence" — a node that *cannot* catch up. Not slow: unable. The
`LocalLog` comment at 513–517 describes the mechanism for a related case: a gap at
`Epoch.FIRST` "which can never be resolved."

CEP-21 is relaxed about lag, and correctly so:

> Any peers themselves may be lagging behind the "true" tail of the log, but this is
> perfectly acceptable, as it is impossible to propagate changes to all participants
> simultaneously, and we achieve correctness by means other than synchrony.

Which is exactly why this property needs the quiet period: lag is fine, permanent lag is not,
and the only way to tell them apart is to stop the faults and wait.

## Code involved

- `tcm/log/LocalLog.java` — `hasGaps()`, `highestPending()`, `waitForHighestConsecutive()`,
  `awaitAtLeast(Epoch)`. The existence of `hasGaps()` as public API is the acknowledgement
  that gaps are a normal transient state.
- `tcm/PeerLogFetcher.java`, `tcm/FetchPeerLog.java`, `tcm/FetchCMSLog.java` — the catch-up
  verbs, tried in that order.
- `tcm/EpochAwareDebounce.java` — coalesces concurrent catch-up requests. `f9e2f1b219`
  ("Properly cancel in-flight futures and reject requests in EpochAwareDebounce during
  shutdown") and `cbf4dcb334` ("Enable EpochAwareDebounce to cancel in flight rpc requests")
  mean this component has had cancellation bugs — and a debouncer that drops a request
  without retrying is a direct route to permanent lag.
- `tcm/MetadataSnapshots.java`, `tcm/transformations/ForceSnapshot.java`,
  `TriggerSnapshot.java` — the skip-ahead path.
- `tcm/Retry.java` — retry deadlines; see CASSANDRA-20059.
- `CMSOperationsMBean.describeCMS()` → `LOCAL_PENDING` (`cms.log().pendingBufferSize()`),
  which is the direct read on "this node has entries it cannot apply."

## What goes wrong if violated

A node permanently behind serves stale ownership and stale schema indefinitely. It is not
detectably broken from the outside — it is up, it answers, its logs show catch-up attempts.
Because it advertises a lower epoch, peers treat it as merely lagging and keep trying to
catch it up, which is indistinguishable in monitoring from a slow node. The cluster is in a
permanently degraded state that no alarm describes.

## Why `Always` inside `eventually_` and not `Sometimes` anywhere

`Sometimes("all nodes converged")` would pass the moment a single timeline converged.
Antithesis runs many timelines; the healthy ones would mask the stuck ones completely, and
the property would report green while the bug it exists to find was occurring. Inside an
`eventually_` command all faults are stopped and killed containers restored, so convergence
is a required outcome and `Always` is the honest choice.

The Antithesis docs are explicit that faults stop immediately but containers need time to
become operational, and that `eventually_` commands "should include retry loops and health
checks." That is not a formality here: after a long partition, catch-up may involve a
snapshot transfer and, for a node mid-bootstrap, streaming.

## The deadline problem

`LOCAL_PENDING` makes the poll smarter than a plain timeout. The recovery loop can
distinguish three states:

- epochs equal across nodes → converged, assert passes;
- epochs unequal and `LOCAL_PENDING > 0` and *decreasing* → making progress, keep waiting;
- epochs unequal and `LOCAL_PENDING` static, or `hasGaps()` with no forward motion →
  genuinely stuck, and waiting longer will not help.

Reporting on the third state rather than on a wall-clock expiry is what keeps this property
from being a flaky-timeout generator. That is the difference between a property that survives
in a suite and one that gets disabled.

## Implementation notes

- Assert both a single distinct epoch *and* a single distinct directory dump. Equal epochs
  with unequal metadata would be an `a-log-prefix-agreement` violation surfacing here, and
  catching it in both places is cheap.
- Exclude nodes deliberately left dead. The workload restarts everything it killed before
  this check (see `b-sequence-resumable-after-crash`), and Antithesis restores its own
  container kills when `eventually_` starts, so "live" should mean "all of them" — if a node
  is still down at that point, that is itself worth reporting.
- Capture per-node `EPOCH`, `LOCAL_PENDING`, `SERVICE_STATE`, and `hasGaps()`-equivalent in
  the failure `Details`. A convergence failure with only "epochs differed" is nearly
  untriageable.

## Local run finding (2026-08-18) — checker false positive, fixed

The first live local run reported this failing once, with `distinct_epochs: 1` but
`distinct_directories: 2`. The epochs *had* converged — all five nodes at epoch 37 — which is the
liveness property this is actually about. The directory-string comparison saw two distinct
renderings only because of the same `InetAddress` self-vs-peer hostname artifact described in
`a-log-prefix-agreement.md`. Fixed the same way: the directory comparison now goes through
`canonicalDirectory()`, which normalises addresses to bare IPs. Checker bug, not a TCM bug — the
cluster converged correctly.

## Investigation Log

#### How long is "eventually"?

- Examined: `TCM_implementation.md`'s Retry section — `tcm_await_timeout` bounds
  `RemoteProcessor` retries, `tcm_rpc_timeout` bounds CMS-local retry attempts;
  `CMSOperationsMBean`'s `getCmsAwaitTimeoutMillis` / `getCmsCommitTimeoutMillis` /
  `getCmsCommitRetryInitialDelayMillis` / `getCmsCommitRetryMaxDelayMillis`; commit
  `b1f30e94f5` "Move long running TCM operations to a longer timout"; `LocalLog.awaitAtLeast`
  and `pendingBufferSize()`.
- Found: every documented bound is per-RPC or per-operation. `b1f30e94f5` existing at all
  confirms the original bounds were too tight for real operations, so any number chosen here
  from first principles would be a guess.
- Not found: any end-to-end convergence bound, in CEP-21, `TCM_implementation.md`, or the
  configuration. There is no such published guarantee — which is reasonable, since it would
  depend on log length, snapshot size, and streaming volume.
- Conclusion: tagged `(partial)`, and resolved into a design decision rather than a number:
  the recovery loop is progress-based rather than deadline-based, using `LOCAL_PENDING` as the
  progress signal, with a generous absolute backstop. First runs will produce real timings;
  the backstop tightens from data, not from guesswork.
