# r-cms-reconfiguration-observed

## What led to this property

CMS reconfiguration is the most safety-critical TCM operation and the least exercised. In
production it happens only during scaling events — and commits `cbe07fd57e` ("Reconfigure CMS
after replacement, bootstrap and move operations") and `e5973bf34f` ("Reconfigure CMS before
assassinate") show it is also triggered *implicitly* as a consequence of other operations,
which makes "did it actually happen in this run?" a genuinely open question rather than
something the workload obviously controls.

Two safety properties are vacuous without it: `c-cms-reconfiguration-quorum-overlap` (an
`AlwaysOrUnreachable`, which passes trivially when never evaluated) and
`c-commit-survives-cms-membership-change`.

## What it guards

- `c-cms-reconfiguration-quorum-overlap` — the CEP-21 bounded-divergence argument.
- `c-commit-survives-cms-membership-change` — CASSANDRA-19872's exactly-once concern.
- Partly `c-cms-membership-never-empty`, whose interesting case is mid-reconfiguration.

## Code involved

- `tcm/sequences/ReconfigureCMS.java`, `AddToCMS.java`, `CancelCMSReconfiguration.java`.
- `tcm/CMSOperationsMBean.java`:
  ```java
  public void reconfigureCMS(int rf);
  public void reconfigureCMS(Map<String, Integer> rf);
  public Map<String, List<String>> reconfigureCMSStatus();
  public void cancelReconfigureCms();
  public void resumeReconfigureCms();
  ```
- `CMSOperations.describeCMS()` → `IS_MIGRATING` (`cms.isMigrating()`) and
  `NEEDS_RECONFIGURATION`.
- `tools/nodetool/CMSAdmin.java` — `cms reconfigure`, made synchronous by default with a
  `--cancel` option in `3acec3c28e`. The workload uses JMX rather than nodetool precisely
  because the JMX call does not block, letting reconfiguration overlap other work.

## Why `Sometimes` on a state, not `Reachable` on a line

The meaningful thing is that a reconfiguration was *in progress* while the workload was doing
something else — `IS_MIGRATING == true`. A `Reachable` on `ReconfigureCMS.advance` would fire
on a reconfiguration that completed instantly between two samples, which proves the code ran
but not that anything was ever exposed to the interleaving the safety properties care about.

## The observation race

`IS_MIGRATING` is sampled by polling, so a fast reconfiguration can begin and end between two
samples and never be observed — the assertion would report "never happened" when it did. Two
mitigations, both used:

- Poll `reconfigureCMSStatus()` as well; a non-empty status is a second, independent signal.
- Have the workload's reconfiguration action record its own before/after member sets. If the
  set changed, a reconfiguration demonstrably occurred even if no sample caught it mid-flight.

The distinction matters for interpretation: "a reconfiguration happened" and "a
reconfiguration was observed in flight" are different claims, and only the second one licenses
confidence in `c-cms-reconfiguration-quorum-overlap`. The assertion is written for the second,
with the first captured as run metadata — because a run where reconfigurations happen but are
never caught in flight needs a slower reconfiguration or a faster sampler, not a passing mark.

## How the workload triggers it

`reconfigureCMS(rf)` with `rf` alternating between values the current node count supports
(e.g. 1 and 3 in a 3–5 node cluster). Alternating is what forces repeated membership movement;
setting it once and leaving it produces one reconfiguration per timeline at most.

Interleaving it with joins and decommissions is deliberate: `cbe07fd57e` means those
operations trigger reconfiguration themselves, so overlapping an explicit reconfiguration with
an implicit one is a state no scripted test produces.

## Implementation notes

- Do not reconfigure to an RF the live node count cannot support — that is a rejection, not a
  reconfiguration, and it would satisfy neither this property nor the ones it guards.
- Clear any in-flight reconfiguration before the `eventually_` recovery check, via
  `resumeReconfigureCms()` or `cancelReconfigureCms()`, so a legitimately-in-progress
  reconfiguration is not misread as a convergence failure.
