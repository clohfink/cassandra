---
sut_path: /Users/clohfink/git/osscass/cassandra
commit: 3c0affbeebaab34cf11b5c8f571bf5f00ba14e0e
updated: 2026-08-14
external_references:
  - path: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-21%3A+Transactional+Cluster+Metadata
    why: Checked whether every stated guarantee has a corresponding property.
  - path: git log --no-merges -- src/java/org/apache/cassandra/tcm
    why: The SUT analysis risk table is derived from it; used here as the coverage checklist.
---

# Evaluation Lens 2 — Coverage Balance

Is this the right *set*? Walked the `sut-analysis.md` failure-prone-areas table row by row, then
checked assertion-type distribution and component spread.

## Coverage of the SUT analysis risk table

| Risk theme (from `sut-analysis.md`) | Properties | Verdict |
|---|---|---|
| Log gaps break catch-up | `a-no-gapped-metadata-published`, `r-snapshot-catchup-used`, `e-cluster-converges-after-faults` | Well covered — 3 properties, appropriate for the top risk |
| Intermediate state visible during replay | `a-epoch-monotonic-per-node` | Covered |
| CMS membership changing under a commit | `c-commit-survives-cms-membership-change`, `r-commit-rejected` | Covered |
| CMS quorum unavailable / unrecoverable | `c-cms-membership-never-empty`, `e-cms-accepts-commits-after-recovery` | Covered |
| Sequence bookkeeping | `b-locked-ranges-match-sequences`, `b-replication-factor-never-under` | Covered |
| Rejection handling | `r-commit-rejected` | Thin — reachability only, no safety property |
| Derived local state drifting from metadata | `d-peers-table-matches-directory` | Covered |
| **Empty/absent placements pre-initialization** | **none** | **GAP** |
| **Retry/timeout policy** | `c-commit-survives-cms-membership-change` (partial) | **Thin** |
| **Schema-change side effects** | `d-schema-agreement-at-same-epoch` (partial) | **GAP** |

## Findings

### F2.1 — No property covers CMS initialization abort or the empty-placements state

**Scope:** catalog-wide gap. Three commits in the risk table have no property at all:

- `ec7794f20f` "Avoid NPE when meta keyspace placements are empty before CMS is initialized"
- `2bc24da841` "Allow empty placements when deserializing cluster metadata"
- `95aca49915` "Avoid NPE during cms initialization abort"

plus the operational surface built for it: `4fb81ea483` added a nodetool command to abort a
failed `cms initialize`, and `CMSOperationsMBean` exposes
`initializeCMS(List<String> ignore)` / `abortInitialization(String initiator)`.

An abort path with its own NPE fix, its own nodetool command, and an `ignore` list on the
initialize call is a state machine, and the catalog does not touch it. Every property in
Category A and C explicitly *guards itself out* of the pre-initialization state
(`epoch >= FIRST`), so the catalog is systematically blind to exactly the window these three
bugs lived in.

**Suggested action:** a property covering the abort/re-initialize cycle. Note the interaction
with F1.1 from the Antithesis Fit lens: this is only reachable if a node re-enters
initialization during the fault-injected phase, which needs the same `wipe-and-restart`
mechanism.

### F2.2 — Prepared-statement staleness has three bug fixes and no property

**Scope:** catalog-wide gap. Three commits, all about the same seam:

- `9bf1680b1f` "Avoid prepared statement invalidation race when committing schema changes" (CASSANDRA-20116)
- `1a6b8e0628` "Invalidate affected prepared stmts on every table metadata change"
- `740879d5a0` "Don't clear prepared statement cache on nodetool cms initialize"

`d-schema-agreement-at-same-epoch` covers schema *version* agreement but not the *effect* — a
prepared statement executed against a stale table definition. That is a client-visible
wrong-answer path, and it is squarely in the thinnest area of the catalog (control plane's
effect on the data path).

The seam is identified precisely in `d-schema-agreement-at-same-epoch.md`:
`notifyPreCommit` fires before the `committed.compareAndSet` and `notifyPostCommit` after, so
anything maintained by a post-commit listener lags the published epoch. Prepared-statement
invalidation is exactly such a listener.

**Suggested action:** a property asserting a prepared statement is never executed against a
table definition older than the statement's own epoch, checked from the workload by preparing
against a probe table, altering it, and re-executing.

### F2.3 — Assertion-type distribution has no `Unreachable`, and there are two obvious candidates

**Scope:** catalog-wide gap. Current distribution across 23 properties:

| Type | Count |
|---|---|
| `Always` | 12 (+3 more inside quiet-period commands) |
| `AlwaysOrUnreachable` | 2 |
| `Sometimes` | 6 |
| `Reachable` | 0 |
| `Unreachable` | 0 |

Zero `Reachable` is fine and deliberate — the catalog consistently prefers `Sometimes` on a
meaningful condition, which the guidance endorses.

Zero `Unreachable` is a genuine gap, because the codebase contains two states that are
explicitly documented as impossible and are currently only detectable as log noise:

1. `LocalLog.java:565` — `IllegalStateException("CAS conflict while trying to commit entry...")`.
   The surrounding comment says "Since we disallow concurrent calls to
   `processPendingInternal`...". Reaching it means the single-caller contract was violated. This
   is the textbook `Unreachable`: a critical internal-invariant failure path. It is also already
   flagged as `(needs human input)` in `a-no-gapped-metadata-published.md`, and an `Unreachable`
   assertion would answer the question empirically instead of waiting on a maintainer.
2. `LocalLog.java:532/538` — `StopProcessingException`. A node that throws this stops applying
   log entries permanently. Its own log message says it "can mean that this node is configured
   differently from CMS," which in a homogeneous harness is impossible by construction.

Both currently surface only as a log line inside a `catch (Throwable t)` (line 575) that logs
"Could not process the entry" and continues — meaning today, a violated core invariant produces
no test failure anywhere.

**Suggested action:** add both as `Unreachable` properties. They are the cheapest high-value
additions available: two call sites, in a file already being edited for three other assertions.

### F2.4 — Rejection handling has reachability coverage but no safety property

**Scope:** property-specific, moderate. `r-commit-rejected` confirms rejections *happen*.
Nothing asserts a rejection is *correct* — that a rejected transformation left no trace. Since
`TCM_implementation.md` states "`Reject`s are not persisted in the log," a natural safety
property is that a rejected transformation never appears at any epoch. This is nearly free: the
workload already maintains a submitted/acked tag ledger for
`c-commit-survives-cms-membership-change`, and rejected-tag-absent is the same check with the
opposite expectation.

**Suggested action:** fold into `c-commit-survives-cms-membership-change` as an explicit
sub-condition rather than a new property — the ledger and the assertion message can carry both
directions without fragmenting the report.

## Component spread

Properties distributed across the topology, checked for concentration:

- **Log/epoch mechanics** (`tcm/log/`, `tcm/Epoch`): 4 properties + 2 proposed `Unreachable`. Appropriate — this is the root of correctness.
- **Sequences/placements** (`tcm/sequences/`, `tcm/ownership/`): 5 properties. Appropriate.
- **CMS** (`tcm/transformations/cms/`, `ReconfigureCMS`): 4 properties. Appropriate.
- **Startup/discovery** (`tcm/Startup`, `tcm/discovery/`, `CMSLookup`): **1 property**
  (`a-metadata-identifier-unique`), and per F1.1 its main scenario is unreachable. Combined with
  F2.1's initialization-abort gap, **startup and discovery are the weakest-covered component**
  despite carrying `eb95b34199`, `f05b27502f`, `4318e74180`, `95aca49915`, and `4fb81ea483`.
- **Request path** (read/write coordination): 3 properties, 1 of which may not fire. Thinnest.

## Passes

- Safety, liveness, and reachability are all represented, with liveness correctly expressed as
  `Always` inside quiet-period commands rather than as `Sometimes`.
- No over-investment found. No low-risk area has disproportionate coverage; the closest is
  Category A with 4 properties, justified because it is the root of everything.
- Cross-cutting concerns are represented rather than falling between focuses:
  `c-commit-survives-cms-membership-change` spans commit + membership;
  `b-progress-barrier-quorum-sound` spans sequences + request path.
- Operational lifecycle transitions are represented (join, leave, move, replace, CMS
  reconfigure) rather than only steady state.

## Uncertainties

- Whether the gossip→TCM upgrade path should count as a gap in *this* catalog or as a
  legitimately separate harness. It is the densest bug area in the history
  (`4318e74180`, `cdfce6b4ac`, `417bb21d2e`, `db94321d71`, `46b90364da`, `1ed52038ce`), which
  argues for urgency, but it needs a different deployment. Recorded as a gap in
  `property-relationships.md`; escalating the priority call to the user.
- Whether streaming failures (`DataMovements`, `LeaveStreams`, `UnbootstrapStreams`) deserve
  their own properties or are adequately covered indirectly by
  `b-sequence-resumable-after-crash`. Streaming is a large subsystem, but its TCM-relevant
  contract is narrow: the sequence must not advance until streaming completes.
