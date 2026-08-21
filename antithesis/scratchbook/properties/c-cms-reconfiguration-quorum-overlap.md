# c-cms-reconfiguration-quorum-overlap

## What led to this property

CEP-21's safety argument for CMS reconfiguration is a *timing* claim, and timing claims are
what a deterministic simulator falsifies for a living:

> divergence cannot grow larger than a single epoch, so any two read or write quorums will
> have overlap

with the conclusion that no additional safety conditions are needed. The protocol is a
two-phase membership move per node: add to the write replica set, stream the entire event
log to the joining node, then add to the read replica set, then run Paxos repair.

Commit `51e01a3862`, "Repair Paxos for the distributed metadata log when CMS membership
changes" (CASSANDRA-20467), is the evidence that this argument was incomplete as first
implemented: data being current was not sufficient, Paxos *state* had to be current too.
That is a fix to the overlap argument itself, not to its implementation details.

Supporting: `6dc9ca99fa` "Retry if node leaves CMS while committing a transformation"
(CASSANDRA-19872) and `7802743460` "Fix issue when running cms reconfiguration with paxos
repair disabled" — the latter meaning the Paxos-repair step is skippable, and skipping it
was broken.

## Code involved

- `tcm/sequences/ReconfigureCMS.java` — the sequence; `advance(context)` is the per-step
  transition and the assertion site.
- `tcm/sequences/AddToCMS.java` — the join half.
- `tcm/CMSMembership.java` — read/write member sets.
- `tcm/sequences/CancelCMSReconfiguration.java` — cancellation, itself a sequence (which
  suggests cancellation is multi-step and therefore has its own windows).
- `tcm/PaxosBackedProcessor.java` — the append that depends on the quorum being sound.
- `tcm/CMSOperationsMBean.java` — `reconfigureCMS(int rf)`, `reconfigureCMS(Map<String,Integer> rf)`,
  `reconfigureCMSStatus()`, `cancelReconfigureCms()`, `resumeReconfigureCms()`. The `Map`
  overload is per-DC RF, unused in a single-DC harness but worth noting as untested surface.

Also relevant: CEP-21 notes "all CMS nodes own an entire range from MIN to MAX token," so
there is no range splitting to reason about — the quorum question is over a flat member set,
which makes the property genuinely simple to state.

## What goes wrong if violated

Non-overlapping read and write quorums means two disjoint sets of CMS nodes can each
linearize an append at the same epoch. That is `a-log-prefix-agreement` failing, and this
property is its root cause rather than its symptom — which is why both are in the catalog.
Detecting it here localises the bug to the reconfiguration sequence; detecting it only via
log disagreement would leave the cause open.

## The window to aim for

Between "added to the write set" and "added to the read set," the joining node is streaming
the entire log. That stream is long — proportional to the log's length — which makes it the
widest window in the whole protocol, and the one where a partition is most likely to land
naturally. Partitioning the joining node mid-stream, then healing, is the primary shape.
Cancelling the reconfiguration mid-stream is the secondary shape, and the one that exercises
`CancelCMSReconfiguration`'s own multi-step nature.

## Implementation notes

- `AlwaysOrUnreachable`: many timelines never reconfigure. Pairing with the
  `r-cms-reconfiguration-observed` reachability property is what makes an all-green result
  on this property meaningful rather than vacuous — without it, "no violations" and "never
  ran" are indistinguishable in the report.
- The condition should be expressed over quorum *sizes*, not member sets:
  `quorum(read) + quorum(write) > |read ∪ write|`. Comparing sets directly invites an
  off-by-one when the sets are equal (the common case, outside reconfiguration).
- `reconfigureCMSStatus()` returns `Map<String, List<String>>` — usable by the workload both
  as the trigger for `r-cms-reconfiguration-observed` and as `Details` content here.

## Investigation Log

#### Does `CMSMembership` expose distinct read and write sets during reconfiguration?

- Examined: `tcm/CMSMembership.java` presence as a `ClusterMetadata` field
  (`ClusterMetadata.java:121`); commit `e1e56e5d5d` "Add CMS membership directly to
  ClusterMetadata" (which is what promoted it to a first-class component); `CMSOperations.describeCMS`
  using `metadata.fullCMSMemberIds()` and `metadata.isCMSMember()`;
  `sequences/ReconfigureCMS.java` and `AddToCMS.java` as the transition implementations;
  CEP-21's two-step add description.
- Found: `fullCMSMemberIds()` is named to distinguish *full* members from partial ones,
  which is exactly the read-set/write-set distinction surfacing in the API — a node in the
  write set but not yet the read set is not a "full" member. So the distinction exists and
  is at least partially exposed.
- Not found: whether `CMSMembership` has direct read-set and write-set accessors, or whether
  the split is only recoverable from `DataPlacements` for the metadata keyspace (which would
  make the assertion read placements instead of membership).
- Conclusion: tagged `(partial)`. The assertion is written against whichever accessor
  exists; the property statement is unaffected. Reading `CMSMembership.java` is a five-minute
  task at implementation time and is called out in the workload notes so it is not
  rediscovered.
