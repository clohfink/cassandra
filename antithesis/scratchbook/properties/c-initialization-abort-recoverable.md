# c-initialization-abort-recoverable

Added by evaluation gap G1 (`evaluation/coverage-balance.md` F2.1).

## What led to this property

A systematic blind spot rather than a single bug. Every Category A and C property guards itself
out of the pre-initialization window with `epoch >= FIRST`, because in that window the CMS
legitimately has no members and `CMS_ID` is legitimately `0`. Those guards are individually
correct and collectively left the catalog unable to see a state machine that has produced three
fixed bugs:

- `ec7794f20f` "Avoid NPE when meta keyspace placements are empty before CMS is initialized"
- `2bc24da841` "Allow empty placements when deserializing cluster metadata"
- `95aca49915` "Avoid NPE during cms initialization abort"

Plus the operator surface built around it, which is the real tell:

- `4fb81ea483` "Add nodetool command to abort failed nodetool cms initialize"
- `f05b27502f` "Improve CMS initialization"
- `CMSOperationsMBean.initializeCMS(List<String> ignore)` and
  `abortInitialization(String initiator)`
- `896d1d6415` "Defer the creation of system_cluster_metadata keyspace until CMS initialization"

A dedicated abort command, an NPE fix *inside* that abort, and an `ignore` parameter on initialize
together describe a multi-outcome operation that operators run under pressure.

## The `ignore` list is the crux

`initializeCMS(List<String> ignore)` proceeds without unanimous participation. From
`TransactionalClusterMetadata.md`, initialization is not a local act:

> one node is chosen for promotion to the initial CMS, which is done manually via nodetool
> `cms initialize`. At this point, the candidate node will propose itself as the initial CMS and
> attempt to gain consensus from the rest of the cluster. If successful, it verifies that all
> peers have an identical view of cluster metadata and initialises the distributed log with a
> snapshot of that metadata.

So initialization involves: propose, gain consensus, verify identical peer views, write the
initial snapshot. Four steps, each with a failure window, and `ignore` explicitly permits
proceeding while excluding some peers. If an abort lands between "gain consensus" and "initialise
the distributed log," some nodes may have accepted the proposal and others not.

## Code involved

- `tcm/transformations/cms/PreInitialize.java` — the `PRE_INITIALIZE_CMS` entry. Note from
  `LocalLog.java:513-517` that `PRE_INITIALIZE_CMS` is one of only two kinds permitted to skip
  epoch gaps, and that the comment describes an unresolvable gap at `Epoch.FIRST` arising from
  `INITIALIZE_CMS` arriving before it.
- `tcm/Startup.java` — startup modes; `tcm/discovery/Discovery.java` — the vote.
- `tcm/CMSOperations.java` — `describeCMS()`, whose `NEEDS_RECONFIGURATION` is
  `metadata.epoch.isBefore(Epoch.FIRST) || needsReconfiguration(metadata)`, and which returns `""`
  for `REPLICATION_FACTOR` when `epoch.isBefore(FIRST)`. Both are the product code's own
  acknowledgement of this window.
- `tools/nodetool/CMSAdmin.java` — `AbortInitialization` (line 231), `InitializeCMS` (96).

## Why the `Epoch.FIRST` gap comment matters here specifically

`LocalLog`'s comment says `INITIALIZE_CMS` is deliberately barred from gap-skipping because
receiving it before `PRE_INITIALIZE_CMS` "creates a gap at Epoch.FIRST which can never be
resolved." That is a permanent, unrecoverable node state, and its precondition is entry reordering
during initialization — which is exactly what a partition during initialization produces.

So this property and `a-no-gapped-metadata-published` meet at the same failure. The guard exists
because someone hit it. Re-attacking it requires being able to run initialization under faults,
which is what this property's setup provides.

## What goes wrong if violated

A cluster where some nodes consider the CMS initialized and others do not. The initialized subset
can commit; the uninitialized subset cannot see those commits as valid. There is no supported
repair: `cms initialize` will be rejected by the nodes that already have a CMS, and
`abortInitialization` cannot undo entries already in the distributed log. Recovery means the
unsafe escape hatch.

The `CMS_ID` asymmetry (`EMPTY_METADATA_IDENTIFIER = 0` on some nodes, non-zero on others) is the
observable signature, which is why the assertion checks uniformity of the triple rather than just
non-emptiness.

## Reachability — and why this property costs more than it looks

Antithesis injects no faults before `setup_complete`, and the harness emits `setup_complete` only
after the CMS is up. So initialization cannot be faulted on the natural path. Reaching this
requires the `wipe-and-rejoin` machinery introduced for `a-metadata-identifier-unique` (evaluation
refinement R4): `unregisterLeftNodes`, wipe the data directory via the control agent, restart.

That shared mechanism is what makes both properties affordable — neither would justify the agent
endpoint alone, together they do.

## Implementation notes

- Assert *uniformity*, not a specific state. Both "all initialized" and "all uninitialized" are
  valid; only the mixture is a violation. Asserting "initialized" would fail on every legitimate
  pre-initialization sample.
- Only compare nodes that answered, and record the count for `h-all-nodes-compared`. A partition
  that hides the disagreeing node would otherwise make this pass.
- The workload's abort action should sometimes abort and sometimes let initialization complete —
  always aborting means the cluster never gets back to a testable state for the other 28
  properties.
