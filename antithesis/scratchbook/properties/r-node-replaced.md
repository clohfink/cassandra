# r-node-replaced

## What led to this property

The workload drove joins (`BootstrapAndJoin`) and decommissions (`UnbootstrapAndLeave`), but never
**replacement** (`BootstrapAndReplace`: `PREPARE/START/MID/FINISH_REPLACE`). Replace is a distinct
multi-step operation — a new node takes over a dead node's token ranges — with its own locked-range
shape and streaming path. It is one of the densest range-movement code paths by commit volume and a
classic source of subtle bugs (streaming the wrong ranges, orphaning a lock, leaving a range
under-replicated, mishandling host_id continuity).

Without a driver for it, every range-movement safety property (`b-*`) and the progress-barrier and
serialization checks were only ever evaluated over join/leave — not over replace.

## Two dead ends found while implementing this (both via local smoke, before spending runner time)

1. **replace-same-address is NOT the replace MSO.** The first implementation killed a ring node and
   re-bootstrapped it in place (`replace_address_first_boot=<self>`). The 6-node smoke showed TCM
   treats this as a re-bootstrap: it commits `PrepareJoin/StartJoin/MidJoin/FinishJoin`
   (`BootstrapAndJoin`), NOT `*_REPLACE`. An assertion keyed on `FINISH_REPLACE` would never fire.
2. **A pre-registered spare cannot be the replacement.** A `join_ring=false` spare registers its own
   address on boot. When it tries `replace_address_first_boot=<victim>` it still uses its *own*
   address, which the cluster already knows, so startup aborts:
   `IllegalStateException: A node with address cassandra-8/… already exists, cancelling join`.
   A real different-address replace needs a node whose address the cluster has **never seen**.

## What actually works: cold spares

Cold-spare containers (`cassandra-11/12/13`) run the node-agent with `NODE_AGENT_AUTOSTART=0`, so
Cassandra is **not started** and the address is never registered. The replace-node driver boots one
with `replace_address_first_boot=<victim>` (the agent also drops `join_ring=false`, a no-op here), and
it bootstraps as a genuine replacement. Confirmed in the local smoke:

```
PrepareReplace.java:125 - Node /172.19.0.7 is replacing /172.19.0.6, tokens [...]
enacted transformations: PrepareReplace, StartReplace, MidReplace, FinishReplace
replace-node: {"victim":"cassandra-5","replacement":"cassandra-11","observed_down":true,"replacement_normal":true}
```

## What it guards

Indirectly the whole `b-*` family plus `b-progress-barrier-quorum-sound`,
`a-metadata-serialization-round-trips`, and `e-cluster-converges-after-faults`, specifically over the
replace path. If this `Sometimes` never fires, an all-green result on those says nothing about
replacement.

## Code involved

- `tcm/sequences/BootstrapAndReplace.java` and the `PREPARE/START/MID/FINISH_REPLACE` transformations.
- `tcm/log/LocalLog.java` — the SUT-side reachability assertion, keyed on
  `kind == Transformation.Kind.FINISH_REPLACE` at the metadata-publication point.
- `antithesis/docker/node-agent.py` — `/replace?address=<victim>` (stops, wipes, drops
  `join_ring=false`, boots with `replace_address_first_boot`). `NODE_AGENT_AUTOSTART=0` makes a cold
  spare.
- Workload `Actions.replaceNode()` + `serial_driver_replace_node` — the driver.
- `Harness.coldSpares` (from `WORKLOAD_COLD_SPARES`) — the never-registered replacement pool, kept
  out of `Harness.nodes` so waitForCluster/checkers ignore them until consumed.
- `Harness.Node`: `stopViaAgent`, `replaceWith`, `broadcastAddress`, `unreachableNodes`.

## Why SUT-side and not workload-side

The concurrency property taught us (run `f863fad2`) that a workload polling JMX can miss a transient
it is trying to observe. `FINISH_REPLACE` is a single enacted transformation; observing it at the
publication point in `LocalLog` is reliable regardless of the driver's poll timing.

## Why `Sometimes` and not `Reachable`

The meaningful thing is that a replacement *completed* — the terminal `FINISH_REPLACE` enacted — not
that a line was hit. A `START_REPLACE` that never finishes (stuck under faults) is exactly the case we
do **not** want to count as success; keying on `FINISH_REPLACE` encodes that.

## Victim/accounting rules

- Victim is `NORMAL`, **non-seed**, and **not a CMS member** (a wiped CMS member cannot fetch the log
  to rebuild — found in the 4-node smoke). Only runs when more than RF nodes are NORMAL.
- Each replace consumes one cold spare and retires one node, so the ring size is unchanged and the
  operation is self-limiting to the number of cold spares (~3/run).
- The retired victim is tagged in the state ledger (`replaced-host:<host>`) so no recovery path
  restarts its now-replaced identity (`clearWorkloadFaults` and the membership-churn recovery branch
  both skip it). It stays down; the checkers already tolerate unreachable/LEFT nodes.
- A replace that stalls under injected faults is left for check-recovery /
  `e-cluster-converges-after-faults` — a real thing to test, not a harness bug.

## Open questions

- Replacing a CMS member (a distinct, more delicate scenario) is deliberately excluded; worth a
  dedicated future driver.
