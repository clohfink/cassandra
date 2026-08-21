# c-cms-membership-never-empty

## What led to this property

Two commits about the same failure class from opposite directions:

- `279c0527aa` "Allow CMS reconfiguration to work around DOWN nodes" — reconfiguration
  could not make progress when members were unreachable, so nodes had to be *worked around*.
  A workaround that removes members has an obvious extremal failure.
- `eb95b34199` "Implement CMS rediscovery and recovery protocol" — the commit body describes
  a real availability cliff: "If a majority of CMS nodes are restarted with new broadcast
  addresses concurrently they are unable to establish a quorum with each other in order to
  commit their address changes." The fix added `TCM_DISCOVER_SURVEY` and
  `TCM_DISCOVER_PEERS` verbs plus `o.a.c.tcm.CMSLookup`.

Both say the same thing: losing the CMS is a real, reachable state, and the codebase has had
to add machinery to climb back out of it.

## Code involved

- `tcm/CMSMembership.java` — CMS membership as a first-class `ClusterMetadata` component
  (added by `e1e56e5d5d` "Add CMS membership directly to ClusterMetadata").
- `tcm/sequences/ReconfigureCMS.java`, `AddToCMS.java`, `CancelCMSReconfiguration.java`.
- `tcm/CMSLookup.java`, `tcm/discovery/` — rediscovery.
- `tcm/CMSOperations.java:213-234` — `describeCMS()`, which is the observation point:

  ```java
  info.put(MEMBERS, members);                     // "(nodeid=N,address=A),..."
  info.put(NEEDS_RECONFIGURATION, ...);
  info.put(IS_MEMBER, ...);
  info.put(SERVICE_STATE, ClusterMetadataService.state(metadata).toString());
  info.put(IS_MIGRATING, Boolean.toString(cms.isMigrating()));
  info.put(EPOCH, Long.toString(metadata.epoch.getEpoch()));
  ```
- `tcm/CMSOperationsMBean.java` — `initializeCMS`, `abortInitialization`, `reconfigureCMS`,
  `cancelReconfigureCms`, `resumeReconfigureCms`. Also `4fb81ea483` added a nodetool command
  to abort a failed `cms initialize`, and `95aca49915` fixed an NPE *during* that abort.

## The legitimate-empty states, which the guard must respect

There are two, and both are real:

1. **Pre-initialisation.** A fresh cluster before the first CMS exists.
2. **Post-upgrade minimal-modification mode.** From `TransactionalClusterMetadata.md`:
   "In this state, the set of allowed cluster metadata modifications is constrained to
   include only the addition, removal and replacement of nodes... **In this mode the CMS has
   no members** and each peer maintains its own `ClusterMetadata` instance independently."

`describeCMS` handles both by reporting `NEEDS_RECONFIGURATION` as true when
`metadata.epoch.isBefore(Epoch.FIRST)`, and returning `""` for `REPLICATION_FACTOR` in that
case. The `epoch >= FIRST` guard on the assertion is therefore the same guard the product
code itself uses, which is the right reason to trust it.

## What goes wrong if violated

No metadata change can ever be committed again. The cluster keeps serving reads and writes
against whatever placements and schema it last had — so monitoring stays green — but DDL
hangs, scaling is impossible, and a failed node cannot be replaced. Recovery requires the
CEP-21 escape hatch: "On loss of a CMS majority, state may be minimally edited to force a
reconfiguration of the CMS without going through the usual consensus process," which the
document itself says "entails a high degree of risk."

This is the worst *operational* TCM failure, distinct from
`a-metadata-identifier-unique`'s worst *correctness* failure.

## Implementation notes

- Parse `MEMBERS` for non-emptiness only; do not try to validate the member set against an
  expectation. During reconfiguration the set legitimately changes, and encoding an expected
  membership would make the assertion fail on correct behaviour.
- Guard on that node's own reported `EPOCH >= 1`. Using a cluster-wide epoch would be wrong:
  a node still catching up after joining is legitimately below `FIRST` in its own view.
- Only assert on nodes that answered. A node that did not answer contributes nothing.
- `SERVICE_STATE` and `IS_MIGRATING` should be captured in the assertion `Details` even
  though they are not part of the condition — during triage they distinguish "lost the CMS"
  from "mid-reconfiguration and observed at an awkward instant."
