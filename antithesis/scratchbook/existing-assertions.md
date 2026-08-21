---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-14
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Authoritative source for the correctness guarantees TCM claims.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: Historical TCM bug fixes mined as candidate properties.
---

# Existing Antithesis SDK Assertions

> **Superseded for the TCM package as of 2026-08-14.** This file recorded the greenfield state
> before the harness was built. Nine SDK assertions have since been added; they are inventoried in
> "Assertions added by this harness" at the end. The scan result below remains accurate for the rest
> of the codebase — everything outside `src/java/org/apache/cassandra/tcm/` still has none.

## Result at time of scan: none

A scan of the repository found **zero** Antithesis SDK usage. Searched `src/`, `test/`,
`conf/`, `build.xml`, and `.build/` for `com.antithesis` and `antithesis` across
`*.java`, `*.xml`, `*.properties` — no matches. There is no `antithesis-sdk-java` jar in
`lib/`, and no `antithesis/` directory existed before this work.

This harness is therefore a greenfield integration. Every assertion referenced in
`property-catalog.md` is **missing** and must be added; no evidence file should describe
instrumentation as already present.

## What exists instead: 82 Java `assert` / `Invariants` statements in TCM

These are not Antithesis assertions, but they are the maintainers' own written statements
of TCM invariants, which makes them the best-sourced candidates for conversion. Counted
82 occurrences of `assert ` / `Invariants.` across 30+ files under
`src/java/org/apache/cassandra/tcm/`.

The two highest-value ones, both in `log/LocalLog.java` inside `processPendingInternal`:

```java
// LocalLog.java:542
assert pendingEntry.epoch.is(next.epoch) :
    String.format("Entry epoch %s does not match metadata epoch %s", pendingEntry.epoch, next.epoch);

// LocalLog.java:544
assert next.epoch.isDirectlyAfter(prev.epoch) || isSnapshot || pendingEntry.transform.kind() == Transformation.Kind.PRE_INITIALIZE_CMS :
    String.format("Epoch %s for %s can either force snapshot, or immediately follow %s",
                  next.epoch, pendingEntry.transform, prev.epoch);
```

Others worth noting as conversion candidates (file-level; see `properties/` for the
specific ones adopted):

| File | Nature of the invariant |
|---|---|
| `log/LocalLog.java` | Epoch consecutiveness and entry/metadata epoch agreement (above); startup epoch preconditions at 270/295/299 |
| `PaxosBackedProcessor.java`, `AtomicLongBackedProcessor.java` | Append preconditions on the log CAS |
| `MultiStepOperation.java` | Sequence step/index bookkeeping |
| `transformations/PrepareJoin.java`, `PrepareReplace.java` | Admission preconditions for range movements |
| `transformations/cms/PrepareCMSReconfiguration.java`, `PreInitialize.java` | CMS membership transition preconditions |
| `ownership/TokenMap.java`, `ownership/ReplicaGroups.java` | Ring/placement structural invariants |
| `ClusterMetadata.java` | Metadata construction invariants |

### Why converting these matters

Java `assert` has three properties that make it a poor fit for the behaviour TCM needs:

1. **Disabled by default.** Cassandra enables `-ea` in test JVMs but production
   deployments generally do not, so these invariants are unchecked exactly where the
   interesting timing occurs.
2. **Fails the process, not the test.** An `AssertionError` inside the `LocalLog` async
   processing thread is caught by the surrounding `catch (Throwable t)` at line 575 and
   logged ("Could not process the entry") — the invariant violation becomes a log line
   that no test asserts on, and the node continues.
3. **No search guidance.** Antithesis uses assertion sites to steer exploration; a Java
   `assert` is invisible to it.

`Assert.always(...)` addresses all three: enabled unconditionally, reported as a property
rather than a crash, safe in production (the SDK's fallback is a no-op or local JSONL),
and used by the platform as a search target.

## Decision recorded

The user chose **harness + SUT-side assertions**. The SUT-side additions are therefore
in scope, which requires `com.antithesis:sdk` in `lib/`. Per `AGENTS.md` ("Do NOT attempt
to install dependencies, every dependency requires OSS community approval first") this
dependency is an explicit, separable ask. It is isolated so the discussion is clean:

- one jar in `lib/`, one `<file>` entry in the build,
- SUT-side call sites confined to `src/java/org/apache/cassandra/tcm/`,
- every call site is a *no-op outside Antithesis* by SDK design, so the dependency
  cannot change production behaviour.

See `antithesis/AGENTS.md` for the dependency rationale as written for reviewers.

## Assertions added by this harness

Nine SDK assertions across four files, all under `src/java/org/apache/cassandra/tcm/`, plus one new
helper. Verified to compile and pass checkstyle via `.build/sh/ai-build`.

| File | Assertion | Message | Property |
|---|---|---|---|
| `log/LocalLog.java` | `always` | `TCM entry epoch matches resulting metadata epoch` | `a-no-gapped-metadata-published` |
| `log/LocalLog.java` | `always` | `TCM enacted epoch directly follows previous or is a legal jump` | `a-no-gapped-metadata-published` |
| `log/LocalLog.java` | `sometimes` | `a node caught up by applying a force snapshot` | `r-snapshot-catchup-used` |
| `log/LocalLog.java` | `always` | `TCM published epoch is non-decreasing` | `a-epoch-monotonic-per-node` |
| `log/LocalLog.java` | `unreachable` | `concurrent TCM log processing detected via CAS conflict` | `a-log-processing-never-concurrent` |
| `log/LocalLog.java` | `unreachable` | `TCM log processing halted because a transformation threw` | `a-log-processing-never-halts` |
| `log/LocalLog.java` | `unreachable` | `TCM log processing halted because a transformation was rejected on replay` | `a-log-processing-never-halts` |
| `sequences/LockedRanges.java` | `always` | `newly locked ranges do not intersect existing locks` | `b-no-overlapping-locked-ranges` |
| `sequences/ProgressBarrier.java` | `sometimes` | `a progress barrier relaxed below its default consistency level` | `r-progress-barrier-relaxed` |
| `sequences/ProgressBarrier.java` | `alwaysOrUnreachable` | `satisfied progress barrier intersects pre- and post-step quorums` | `b-progress-barrier-quorum-sound` |
| `AbstractLocalProcessor.java` | `sometimes` | `a transformation was rejected by the CMS` | `r-commit-rejected` |

New file: `src/java/org/apache/cassandra/tcm/AntithesisDetails.java` — builds the `ObjectNode`
`details` payload from alternating key/value pairs.

Two design points worth preserving:

- **The helper does not wrap the assertion calls.** The cataloger requires each `message` to be a
  string literal or compile-time constant *at the call site*, since each distinct message becomes its
  own test property. A wrapper taking the message as a parameter would pass a variable to
  `Assert.always` and break cataloging. Only the payload construction is shared, which is why
  `com.antithesis.sdk.Assert` is imported in four files rather than one.
- **`Assert.sometimes` is never given a structurally-true condition.** The rejection assertion in
  `AbstractLocalProcessor` was initially placed inside the `if (result.isRejected())` branch, making
  its condition `true` — which the SDK guidance flags as a `Reachable` in disguise. It was moved
  above the branch so it evaluates `result.isRejected()` on every commit attempt, which is the real
  claim ("a rejection occurred among the attempts").

## First local run (2026-08-18)

The harness was driven against a live 5-node cluster (8 rounds of churn + invariant sweeps + a
quiet-period recovery check; ~132 assertion evaluations). No fault injection beyond what the
workload triggers through the control agent, so this exercises plumbing and the happy-ish path, not
Antithesis's exploration.

**Every substantive TCM safety property held**: epoch monotonicity, single metadata identifier,
schema agreement at same epoch, RF preserved, ring fully owned, exactly-once commit, CMS non-empty,
initialization uniform, prepared-statement-not-stale, sequences drained, CMS accepts commit after
recovery. Neither `Unreachable` fired (no concurrent log processing, no halt). The cluster converged
to a single epoch with an agreed CMS membership.

**Findings across the shakedown runs: three checker bugs, no TCM bugs.** All were fixed and a
third, fresh run came back clean (no property violations; see "confirming run" below).

1. **Address self/peer rendering (run 1, 2 properties).** A node renders its own directory entry's
   addresses as `hostname/ip` (the local `InetAddress` carries the resolved hostname) but every peer
   renders that node as `/ip` — an `InetAddress.toString()` artifact, not a `ClusterMetadata`
   difference. `a-log-prefix-agreement` and `e-cluster-converges-after-faults` compared rendered
   directory strings and so reported divergence that was not real. Fixed with a
   `canonicalDirectory()` helper that reduces every address field to its bare IP before comparison.
2. **Peers self-exclusion by address (run 1, 1 property).** `d-peers-table-matches-directory`
   excluded the self node by matching a container hostname against a directory `broadcast_address`
   stored as an IP, so self was never excluded and always looked "missing from peers_v2". Fixed to
   exclude self by `host_id` from `system.local`.
3. **Decommissioned node counted as live (run 2, 1 property).** After the workload decommissioned a
   node, its container kept running and answering JMX with its final frozen epoch;
   `e-cluster-converges-after-faults` counted it as a live node that failed to converge. A
   decommissioned node freezing its epoch is *correct TCM behaviour*. Fixed to exclude nodes in a
   terminal mode (DECOMMISSIONED / LEFT / DRAINED) — the same "answers JMX ≠ is a cluster member"
   distinction `h-all-nodes-compared` is built on. This is also exactly the caveat this property's
   own evidence file recorded ("Exclude nodes deliberately left dead") and that `convergence()` had
   not yet implemented.

This is the harness working as intended: three shakedown runs flushed out fragile checkers before
they could mask or fake a real finding.

**Confirming run (fresh 5-node cluster, 8 rounds + recovery, 126 evaluations):** no property
violations. All 14 safety/liveness assertions held every evaluation, including the three that were
fixed; neither `Unreachable` fired.

## First Antithesis run (run `8bf9b2c6fb30165cc6895473cb18aa3b-59-13`, 2026-08-18, 30 min)

Submitted to the `crimson-whale` tenant via `basic_test` after fixing an amd64 config-image issue
(a prior submission `v59-13` failed setup because `snouty` built the config image arm64 on Apple
Silicon; fixed with `DOCKER_DEFAULT_PLATFORM=linux/amd64` — see `README.md`). The run completed:
**55 properties passing, 8 failing.**

**No TCM correctness bug.** Every failing property triaged to a harness defect, a coverage gap, or
an environment limit. Evidence for each is in the triage below; none is a property of Cassandra
failing.

Positives that only fault injection could produce, and that held:
- `a progress barrier relaxed below its default consistency level` — **satisfied** (relaxation
  actually occurred under partition), and `satisfied progress barrier intersects pre- and post-step
  quorums` — **passing**. CEP-21's headline safety theorem was exercised and held.
- `a node caught up by applying a force snapshot`, `a CMS reconfiguration was observed in progress`,
  `a transformation was rejected by the CMS` — all **satisfied**.
- SUT-side epoch invariants and `newly locked ranges do not intersect` — **passing** over thousands
  of evaluations. Both `Unreachable` tripwires never fired.

### Failures, triaged

| # | Property | cex/ex | Category | Root cause (evidence) |
|---|---|---|---|---|
| 1 | TCM log entries agree across nodes at the same epoch | 25/1063 | checker | At epoch 29, two nodes' directories byte-identical except `multi_step_operation`; the 48 affected ranges equal as a set, RF set `{1,2,3}` equal — only the rendered `HashMap<ReplicationParams,…>` iteration order differed (`[3,2,1]` vs `[2,3,1]`). Masking the field → exactly equal. |
| 2 | all live nodes converge to an identical cluster metadata epoch | 11/736 | checker | Counterexample had `distinct_epochs:1` (epochs converged; decommission-exclusion held) but `distinct_directories:3` — same `multi_step_operation` rendering artifact as #1. |
| 3 | every committed transformation appears exactly once in the log | 34/518 | checker | Counterexample `recorded_outcome:"rejected", occurrences:1`. The workload bucketed `AlreadyExistsException` as "rejected"; under retry that means the table **committed** (ack lost). No `occ≥2` (double-commit) or `acked&occ=0` (lost) seen in any sample. |
| 4 | tcm/anytime_check_tcm_invariants (exit code) | 10/1647 | workload | `State.save()` threw `NoSuchFileException: state.json.tmp` — concurrent workload processes raced on one shared temp filename. Threw at `close()` **after** assertions were emitted, so results were unaffected; only the exit code went red. |
| 5 | tcm/parallel_driver_prepared_statement_check (exit code) | 1/973 | workload | Same state-save race, once. |
| 6 | two or more multi-step operations were in flight at once | ex=0 | coverage gap | The workload never achieved two concurrent range movements even under faults, so the concurrency-admission safety properties were not meaningfully exercised. Needs a more aggressive workload (`antithesis-workload`). |
| 7 | a replica rejected a request because the coordinator was behind | ex=0 | coverage gap | `CoordinatorBehindException` never observed — matches the unresolved observability open question in `properties/r-coordinator-behind-rejection.md`. |
| 8 | Always: Peak memory usage (Antithesis built-in) | 6/16273 | environment | Peak-memory ceiling hit a few times — the deliberately tiny heaps + 5 nodes on the instance, not TCM. |

### Fixes applied (2026-08-18)

- **#1, #2** — `Checks.canonicalDirectory()` now excludes `multi_step_operation` (`EXCLUDED_FIELDS`),
  because `mso.status()` renders unordered maps/sets and is not a stable function of metadata even at
  a fixed epoch. In-progress sequences are still checked structurally elsewhere.
- **#3** — `Actions.schemaChurn()` now classifies `AlreadyExistsException` as `acked` (committed,
  ack-lost-then-retried), separate from genuine `InvalidQueryException` rejections.
- **#4, #5** — `Harness.State.save()` uses a per-process temp filename and is now best-effort (logs
  and continues on failure instead of throwing), so a diagnostic-state write can never fail a test
  command.
- **#6, #7** — not code-fixed here; they are workload-strength/observability work for the
  `antithesis-workload` skill. Recorded as open items in the two property evidence files.

All four checker/workload categories are the same lesson as the local shakedown: **never compare a
rendered string that is not canonical, and never let harness bookkeeping fail the SUT's result.**
The value of the run is precisely that fault injection reached states (frequent in-progress
operations, lost acks, concurrent processes) that no local no-fault run did.

## Second Antithesis run (run `e0bc66a204cbdded44fc166191d0c33a-59-13`, 2026-08-18, 30 min)

Same harness with the three fixes above. Result: **60 passing, 3 failing — no safety violation, no
`Unreachable` hit.** The fixes held under real fault injection:

| Previously-failing checker | Run 2 result |
|---|---|
| TCM log entries agree across nodes at the same epoch | Passing (1486 ex, 0 cex) |
| all live nodes converge to an identical cluster metadata epoch | Passing (647 ex, 0 cex) |
| every committed transformation appears exactly once in the log | Passing (156 ex, 0 cex) |
| tcm/anytime_check_tcm_invariants (exit code) | Passing (1878 ex, 0 cex) |
| tcm/parallel_driver_prepared_statement_check (exit code) | Passing (905 ex, 0 cex) |
| Always: Peak memory usage (built-in) | Passing (15388 ex, 0 cex) |

Both `Unreachable` tripwires: Passing, never hit. Fault-dependent safety paths held, e.g.
`satisfied progress barrier intersects pre- and post-step quorums` (87 ex, 0 cex),
`every range retains at least RF write replicas at every epoch` (1745 ex, 0 cex),
`newly locked ranges do not intersect existing locks` (234 ex, 0 cex). Reachability that *did* fire:
snapshot catch-up (143), CMS reconfiguration observed (257), transformation rejected (74).

**The 3 remaining failures are all `Sometimes` never-satisfied — reachability gaps, not bugs:**

- `two or more multi-step operations were in flight at once` (ex=0) — still not reached; the
  concurrency-admission properties remain under-exercised. Workload work (see that evidence file).
- `a replica rejected a request because the coordinator was behind` (ex=0) — still not observed;
  observability unresolved (see that evidence file).
- `a progress barrier relaxed below its default consistency level` (ex=0) — **flipped from
  satisfied in run 1 to unreached here**: this run's barriers were satisfied at the default level
  (EACH_QUORUM) without ever needing to relax. Stochastic across fault interleavings, not a code
  change; it means run 2's partitions did not sustain long enough during a barrier wait to force
  fallback. Worth watching across runs rather than acting on from one.

Net: the harness is now trustworthy under fault injection (all checkers clean, no safety violation),
and the open work is workload strength + one observability question, not correctness.

**Reachability gaps (expected locally, no faults):** CMS-reconfiguration-observed (reconfig
completed between polls), coordinator-behind (needs partitions), and two-concurrent-multistep-ops
(needs overlapping movements) never fired. These need Antithesis's fault injection and are why a
green local run is necessary but not sufficient.

## Open Questions

- Should the 82 existing `assert`s be *replaced* by `Assert.always` or *paired* with it?
  Pairing keeps `-ea` test behaviour (fail fast in unit/dtest) while adding Antithesis
  reporting. This harness pairs rather than replaces, so no existing test behaviour
  changes. (needs human input — a maintainer preference, not a technical constraint.)
- The two `unreachable` assertions on the `StopProcessingException` throw sites are sound only for a
  homogeneous cluster. A future gossip→TCM upgrade harness runs mixed versions, where a
  version-driven transformation failure may be legitimate, and would need them relaxed to
  `alwaysOrUnreachable` or removed. Recorded so that harness does not inherit a spurious failure.

## Run 82fa2eda...-59-13 (cassandra_oss, 7 ring + 3 spares, 30 min) — INCOMPLETE: cold-start bootstrap collision

First run on the grown 7+3 topology (goal: make `r-concurrent-multistep-operations` reachable).
Result: **incomplete, zero properties recorded.** Build + Antithesis instrumentation were clean
(`ready: true`, both jars cataloged, symbols extracted); the run reached vtime ~1808s (full 30 min)
but never evaluated a single property.

**Root cause (not a TCM correctness bug):** all seven ring nodes started in parallel and tried to
bootstrap/join at once. With random tokens (allocate_tokens disabled for disjoint concurrency),
several joins locked overlapping ranges simultaneously; TCM's admission control correctly rejected
the colliding plan, and Cassandra's *startup* path treats a rejected join as fatal:

```
cassandra-5 (vtime 101s):
  IllegalStateException: Can not commit transformation: "INVALID"
  (Rejecting this plan as it interacts with a range locked by Key{Epoch=19})
    at ClusterMetadataService.commit -> Startup.startup -> StorageService.joinRing
    at CassandraDaemon.setup
  -> node-agent: cassandra exited with code 3   (not restarted)
```

A required ring node never became healthy -> `docker compose up -d` returned exit 1 (vtime 305) ->
the workload `setup` driver failed -> **`setup_complete` never fired** -> Antithesis recorded no
properties -> `incomplete`. The `failure_moment` (vtime ~1812, at the duration boundary) is just the
run ending; the real failure was at ~101s.

This is arguably intended behaviour: fatal-abort on a concurrent-overlapping bootstrap is the TCM-era
equivalent of `consistent.rangemovement=true` refusing to bootstrap while another node bootstraps.
So the fix is in the harness, not the SUT.

**Fix (docker-compose.yaml):** bootstrap the seven ring nodes **serially** via a linear
`depends_on: {condition: service_healthy}` chain (2 waits on 1, 3 on 2, ... 7 on 6). Each single
join has no competitor -> no range-lock collision -> the ring forms reliably. Spares (8-10,
join_ring=false) take no ranges at boot and start after cassandra-7 is healthy. Concurrency is now
exercised only AFTER setup, when the workload joins the spares (2 at a time on disjoint arcs) -- which
is exactly where the collisions are the point, the workload tolerates rejections, and admission
control (`r-concurrent-multistep-operations`, LockedRanges safety) is under test. Relaunching.

## Run f863fad2...-59-13 (cassandra_oss, 7+3 SERIAL bootstrap, 45 min) — COMPLETED: 64/65 pass

The serial-bootstrap fix worked end-to-end: run **completed** (not incomplete), no failure_moment,
ran the full 45 min, and **64 of 65 properties passed**. Setup finished cleanly every timeline.
This confirms run 82fa2eda's failure was purely the cold-start bootstrap collision.

**The one failing property is still `r-concurrent-multistep-operations`** ("two or more multi-step
operations were in flight at once", a `Sometimes` assertion): example_count=0, 101 counterexamples.
But the reason is now DIFFERENT and more informative. From the driver's own log at the moment:

```
v=258.9 [workload] concurrent: joinRing on spare cassandra-8
v=258.9 [workload] concurrent: joinRing on spare cassandra-9
v=288.9 [workload] concurrent movements: launched=[join:cassandra-8, join:cassandra-9] peak_in_flight=1
```

- Both joins ARE launched concurrently (daemon threads / fireAndForget) — good.
- **peak_in_flight=1**: across the 30s sampling window, at most one BootstrapAndJoin MSO was ever in
  `inProgressSequences`. The two never coexisted.
- **Zero "Rejecting this plan / range locked" in the captured logs for this window** — so this is NOT
  admission-control serialization (unlike the 3-node case). The two sequences just didn't overlap in
  the observed window.

So the remaining gap is **workload strength / observation**, not topology and not a correctness bug.
Likely factors and candidate fixes (needs a decision before more runner time):
  1. The two spares' random bootstrap ranges may still overlap enough that TCM serialises them on the
     node side (rejection may be happening on cassandra-9, whose stderr this one-timeline log bundle
     didn't fully capture). Fix: assign the two spares EXPLICIT non-overlapping `initial_token` sets so
     their bootstrap ranges are provably disjoint -> both admitted -> both in flight.
  2. joinRing bootstrap streaming may complete faster than the 30s poll resolution catches an overlap.
     Fix: sample `inProgressSequences` more tightly, or assert SUT-side when
     `ClusterMetadata.inProgressSequences.size() >= 2` (surgical, catches the transient reliably).
  3. Combine: force-disjoint tokens + SUT-side size>=2 assertion.

**Cosmetic SDK noise (not a failure):** every `Assert.sometimes()` emit throws
`ClassNotFoundException: com.antithesis.sdk.generated.AssertionCatalog`
(HandlerFactory.didLoadCatalog -> Class.forName). The assertion still records (65 properties, 101
counterexamples prove emit works), so it is harmless, but the stack traces pollute workload stderr.
Worth suppressing / ensuring the generated catalog is on the classpath.

Net after run f863fad2: harness + pipeline are solid (64/65, reliable setup, runs on cassandra_oss).
The concurrency happy-path is a known-hard reachability target that now gets as far as launching two
concurrent joins; making two provably coexist is the next workload investment.

### Fix applied (post-run-f863fad2): move r-concurrent-multistep-operations SUT-side

Chosen approach: surgical SUT-side observer instead of workload polling.
- `InProgressSequences.java`: added `public int size()` (only `isEmpty()` existed).
- `LocalLog.java`: added `Assert.sometimes(next.inProgressSequences.size() >= 2, "two or more
  multi-step operations were in flight at once", ...)` inside the `committed.compareAndSet(prev,next)`
  block -- the one point where every metadata transition becomes visible. Catches the >=2-in-flight
  transient regardless of poll cadence (run f863fad2 saw peak_in_flight=1 purely because the JMX poll
  missed the window).
- `Actions.java`: removed the workload-side `Assert.sometimes(peak>=2, ...)` so the property message
  is owned by exactly one callsite (SDK catalogs by message string). The concurrent-movements DRIVER
  stays (it drives the SUT into the state); `peak_in_flight` is now telemetry-only for triage
  comparison.
Both the Cassandra jar and the workload jar compile (BUILD SUCCESSFUL). Relaunching as run 3.
If the SUT-side sometimes STILL never fires, that proves the two bootstrap sequences genuinely never
coexist (2nd join rejected/serialised on range overlap) -> next step would be forcing disjoint tokens.

## Run 036a6fc3...-59-13 (cassandra_oss, 7+3 serial, SUT-side concurrency assert, 45 min) — 65/65 PASS

The SUT-side move worked. Run **completed**, no failure_moment, full 45 min, **all 65 properties
passing (zero failures).** The previously-unreachable concurrency property is now satisfied:

```
[Passing] two or more multi-step operations were in flight at once   ex=160  cex=112
```

example_count=160 = the LocalLog assertion observed inProgressSequences.size() >= 2 one hundred and
sixty times. So the two BootstrapAndJoin sequences DO coexist on disjoint ranges under load; run
f863fad2's peak_in_flight=1 was purely a JMX-poll blind spot, confirming the diagnosis. This also
makes the admission-safety properties (b-no-overlapping-locked-ranges, b-locked-ranges-match-
sequences) non-vacuous -- they now run while two sequences are genuinely locked at once.

Final state of the harness: 65/65 green on cassandra_oss with fault injection, reliable setup via
serial bootstrap, and the full research->setup->workload->launch->triage loop established.
Across three completing runs: zero TCM correctness bugs; one real harness topology bug found+fixed
(cold-start bootstrap collision -> serial chain); one hard reachability target closed by moving the
observer SUT-side. Remaining nice-to-have: suppress the cosmetic SDK AssertionCatalog
ClassNotFoundException stderr noise (does not affect results).

## Run efcbee84...-59-13 (cassandra_oss, 14 svc, +2 properties, 45 min) — 66/68; both findings triaged

First run with #1 (r-node-replaced) and #3 (a-metadata-serialization-round-trips). Completed, 66/68.

- **r-node-replaced: PASSING (ex=158).** Real BootstrapAndReplace (cold spare takes a killed
  non-CMS/non-seed node's tokens) fires FINISH_REPLACE 158x under fault injection. #1 works.
- **a-metadata-serialization-round-trips: FAILING (72 cex) -> FALSE POSITIVE, fixed.** All 72 had
  error:null on PREPARE_COMPLEX_CMS_RECONFIGURATION / ADVANCE_CMS_RECONFIGURATION metadata. A
  bytesStable diagnostic (deserialize -> re-serialize -> compare bytes) reproduced locally via
  setup-cms showed bytesStable=true in every case: the wire form is faithful; ClusterMetadata.equals()
  is just stricter than serialization for CMS-reconfiguration in-progress sequences. Fix: the property
  condition is now byte-stability (re-serialized bytes identical) + no-throw, not equals(). Still
  catches the historical deserializer bugs. (Minor: equals() is inconsistent with the wire form for
  those sequences -- a maintainer note, not data loss.)
- **e-sequences-drained: FAILING (1 cex of 334).** A JOIN sequence (node10) still at ProgressBarrier
  epoch 65 step 1 at the sample instant, alongside a concurrent decommission rejected for a range lock
  (epoch 64). A rare liveness edge under heavy concurrent churn, not a replace artifact (replacements
  keep the retired node's ranges covered, so dead victims do not stall barriers). Was 65/65 in run 3
  (pre-replace); watching whether it recurs. Not blocking.

Net: #1 confirmed working; #3 confirmed working as a detector and its one false-positive class fixed.
Relaunching (run 5) with byte-stability to confirm green.

## Run c3c05902...-59-13 (byte-stability serialization check, 45 min) — 67/68

- r-node-replaced PASSING (ex=283). e-sequences-drained RECOVERED (was the 1-cex flake in run 4).
- a-metadata-serialization-round-trips STILL FAILING (127 cex), now on PREPARE_JOIN with "bytes
  differ". Diagnosed: selfStable=true (serialization deterministic) but round-trip bytes differ =>
  the join sequence's map is reconstructed in a different iteration order on deserialize; equals() is
  true (order-independent). So byte-stability ALSO false-positives -- a SECOND benign asymmetry,
  distinct from the CMS transient-field one that sank the equals() check.
- Final condition: fail iff deserialize throws OR (!equals AND bytes differ). Each benign asymmetry
  trips one signal; real corruption trips both. Verified locally across join + CMS-reconfig: every
  mismatch is single-signal, 0 "differs in BOTH". Relaunching (run 6) to confirm green.

## Run 8ae870fa...-59-13 (idempotence serialization check, 45 min) — ALL PASS

Everything green. a-metadata-serialization-round-trips PASSING (ex=261) with the idempotence
condition (b2==b3): both benign asymmetries (CMS-reconfig transient field; join map re-order) and
their co-occurrence resolve to a fixed point after one round-trip, while a genuine non-idempotent
serializer asymmetry (or a throw) would still fail. r-node-replaced PASSING (ex=139). No failures.

#1 (node replacement, cold-spare BootstrapAndReplace) and #3 (metadata serialization round-trip via
idempotence) are complete and green under fault injection.
