# a-log-processing-never-halts

Added by evaluation gap G3 (`evaluation/coverage-balance.md` F2.3).

## What led to this property

`StopProcessingException` is thrown from two places in
`LocalLog.processPendingInternal`, and both messages blame configuration divergence:

```java
try
{
    transformed = pendingEntry.transform.execute(prev);
}
catch (Throwable t)
{
    logger.error("Caught an exception while processing entry {}. This can mean that this node is configured differently from CMS.", prev, t);
    throw new StopProcessingException(t);            // line 532
}

if (!transformed.isSuccess())
{
    logger.error("Error while processing entry {}. Transformation returned result of {}. This can mean that this node is configured differently from CMS.", prev, transformed.rejected());
    throw new StopProcessingException();             // line 538
}
```

In a harness where every node runs the same image and the same `cassandra.yaml`, "configured
differently from CMS" is impossible by construction. That makes `Unreachable` the correct
assertion type rather than a judgement call.

Note the second site especially: a transformation that the CMS accepted as a `Success` returning
a `Reject` when replayed locally. Transformations are supposed to be pure functions of
`ClusterMetadata` — `Transformation` is described as a "side-effect free function mapping an
instance of immutable `ClusterMetadata` to next `ClusterMetadata`". If the same function on the
same input disagrees between CMS and peer, then either the input differed (which is
`a-log-prefix-agreement` failing) or the function is not pure.

## Why this is distinct from the halt symptom

`StopProcessingException` is re-thrown rather than swallowed — it is caught at line 571 and
rethrown, unlike the `IllegalStateException` in `a-log-processing-never-concurrent` which is
absorbed by the generic handler. So it genuinely propagates and stops the processing loop.

The consequence is a node frozen at its current epoch, permanently, while remaining up and
serving reads and writes against stale metadata. That state *would* eventually be caught by
`e-cluster-converges-after-faults` — but as "node 3 did not converge," several inferential steps
away from "node 3's transformation execution disagreed with the CMS's." Asserting at the throw
site names the cause instead of the symptom, which is the difference between a triageable finding
and a research project.

## How it could be reached

The interesting route is not misconfiguration. It is a peer applying an entry against a base
state the CMS never had:

- Snapshot-based catch-up (`r-snapshot-catchup-used`) constructs a synthetic `ForceSnapshot`
  entry and jumps the node's state forward. Subsequent real entries are then executed against
  that snapshot-derived state rather than against the state the CMS had when it accepted them.
- The `FORCE_SNAPSHOT` comparator priority means a snapshot can preempt queued entries, and
  "any entry with epoch lower than the one that snapshot has enacted, are simply going to be
  dropped." The interleaving of dropped entries and applied ones is where a divergent base state
  could arise.

That makes this property a natural partner to `r-snapshot-catchup-used`: the snapshot path is the
most plausible producer of the state this assertion declares impossible.

Secondary route: a transformation whose execution depends on local state rather than only on the
passed `ClusterMetadata`. That would be a purity violation, and purity is asserted in prose, not
in code.

## What goes wrong if violated

Silent permanent staleness on one node. It answers queries, it appears in gossip, monitoring shows
it up. Its ownership and schema view are frozen at the moment it halted. Because it advertises a
lower epoch, peers treat it as merely lagging and keep trying to catch it up — indistinguishable
in monitoring from a slow node. It will never recover without a restart, and a restart replays
from persisted state and may halt again at the same entry.

## Implementation notes

- Two `Assert.unreachable` calls, one per throw site, with **distinct messages** — one for
  "transformation threw", one for "transformation rejected". The guidance is explicit that
  assertion messages must be unique per callsite, and here the distinction is diagnostically
  important: a throw is a bug in the transformation, a rejection is a divergent base state.
- `Details` should carry the entry kind, `prev.epoch`, and for the rejection case
  `transformed.rejected()`. The rejection reason is the single most useful field for triage.
- Place before the `throw`, so the assertion fires regardless of how the exception is handled
  upstream.

## Open question worth carrying forward

In a mixed-version cluster mid-upgrade, `StopProcessingException` may be legitimate — a node
running an older version could genuinely be unable to execute a newer transformation. This harness
is homogeneous, so `Unreachable` is sound here, but a future upgrade harness would need this
relaxed to `AlwaysOrUnreachable` or removed. Recorded so that whoever builds the upgrade harness
does not inherit a spurious failure and conclude the harness is broken.

### Investigation Log

#### Is there a legitimate operational reason for `StopProcessingException` in a heterogeneous cluster?

- Examined: both throw sites and their log messages (`LocalLog.java:529-539`); the rethrow at
  571–574 distinguishing it from the swallowed `IllegalStateException`; `Transformation.java`'s
  purity description in `TCM_implementation.md`; the `NodeVersion`/`serialization_version` columns
  in `cluster_metadata_directory`, which show the cluster tracks per-node versions.
- Found: the existence of `serialization_version` per node in the directory confirms
  version-heterogeneity is a modeled state, which makes a version-driven execution failure
  plausible in principle. The log messages' wording ("configured differently") points at
  configuration rather than version, but does not exclude it.
- Not found: any code path that distinguishes a version-incompatibility failure from a
  configuration-divergence failure at these throw sites — both produce the same exception.
- Conclusion: tagged `(partial)`. Sound for this homogeneous harness; flagged for the upgrade
  harness. Resolving it fully would need the upgrade path, which is out of scope here.
