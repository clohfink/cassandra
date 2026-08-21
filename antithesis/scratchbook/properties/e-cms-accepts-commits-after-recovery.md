# e-cms-accepts-commits-after-recovery

## What led to this property

Commit `eb95b34199`, whose body describes a complete metadata-plane outage from an ordinary
operational event:

> If a majority of CMS nodes are restarted with new broadcast addresses concurrently they are
> unable to establish a quorum with each other in order to commit their address changes.

A deadlock: the address changes need a quorum to commit, and the quorum needs the address
changes to form. The fix added `TCM_DISCOVER_SURVEY` and `TCM_DISCOVER_PEERS` verbs and
`o.a.c.tcm.CMSLookup` to build a temporary address mapping — "sufficient for the CMS members
to form a quorum and begin committing the address changes to the metadata log."

`e-cluster-converges-after-faults` checks that everyone agrees. This one checks something
strictly stronger and separate: that the cluster can still *change*. A cluster can be
perfectly converged and permanently unable to commit anything.

## Code involved

- `tcm/CMSLookup.java` — the rediscovery mapping.
- `tcm/discovery/` — `Discovery` and the discovery verbs.
- `tcm/Startup.java` — startup modes, including the rediscovery path.
- `tcm/PaxosBackedProcessor.java` — the append that must succeed.
- `tcm/CMSOperationsMBean.java` — `getCommitsPaused()` / `setCommitsPaused(boolean)`. The
  workload uses `setCommitsPaused(true)` as a deliberate fault and must ensure it is cleared
  before this check, or the property fails for a reason the workload caused.
- `CMSOperations.describeCMS()` → `NEEDS_RECONFIGURATION`, `SERVICE_STATE`, `COMMITS_PAUSED`.

Related history: `4fb81ea483` (nodetool command to abort a failed `cms initialize`),
`95aca49915` (NPE during initialization abort), `f05b27502f` ("Improve CMS initialization"),
`279c0527aa` (reconfiguration around DOWN nodes). The CMS's ability to accept commits has
needed repeated repair.

## What goes wrong if violated

The cluster serves reads and writes normally — every dashboard is green — but no metadata
change can be committed. No DDL. No scaling. No node replacement, which means the *next*
hardware failure is unrecoverable. It is the most dangerous shape of outage because nothing
alerts: the symptom is a hung `nodetool` command that an operator discovers by trying.

CEP-21's escape hatch is the only recovery, and it says of itself that its use "entails a
high degree of risk."

## Why a real DDL rather than a probe

The commit must go through the same path an operator's change would: client → `RemoteProcessor`
→ CMS → Paxos append → replication → visible on all nodes. A synthetic health check that only
verifies a CMS quorum is reachable would miss `RemoteProcessor` retry exhaustion, the commit
member preference policy (`9a896cbaeb` added `getCmsCommitMemberPreferencePolicy`), and the
epoch-visibility half. `CREATE TABLE` on a dedicated probe keyspace, then confirming the
resulting epoch is visible on every node, exercises all of it.

Asserting visibility and not just acceptance matters: a commit that appends but never
replicates leaves the cluster in the state `e-cluster-converges-after-faults` covers, and
checking both here makes the two properties fail distinguishably.

## Implementation notes

- Clear workload-induced faults first: `setCommitsPaused(false)` on every node, and restart
  any node the workload stopped. Otherwise this property reports the workload's own actions
  as defects — the fastest way to make a liveness assertion untrustworthy.
- Retry with backoff. Antithesis stops faults when `eventually_` begins but containers need
  time; a single attempt would fail on recovery latency rather than on a real defect.
- Use a fresh, uniquely-named table per attempt so a retry cannot be satisfied by a
  previously-committed change. Reusing a name makes `IF NOT EXISTS` semantics silently turn a
  failed commit into an apparent success.
- Include `NEEDS_RECONFIGURATION` and `SERVICE_STATE` per node in the `Details`. If the CMS
  needs reconfiguration and cannot get it, that is a materially different diagnosis from a
  quorum that is present but not committing.
