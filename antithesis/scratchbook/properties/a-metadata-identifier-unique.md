# a-metadata-identifier-unique

## What led to this property

`ClusterMetadata` carries a `metadataIdentifier` field with a sentinel
`EMPTY_METADATA_IDENTIFIER = 0`, and `CMSOperations.describeCMS()` surfaces it as `CMS_ID`.
A dedicated identifier for "which metadata service produced this log" only earns its place
if the system considers it possible to encounter entries from a *different* one — i.e. it
is a split-brain guard. Guards imply the guarded event was considered reachable.

CEP-21 reinforces this: a new or upgraded cluster deliberately starts "with a single CMS
node," explicitly to avoid "complex try-and-backoff initialisation protocols." Choosing
simplicity at initialisation means the safety burden shifts to the identifier check and to
`Discovery`'s refusal to form a second service.

## Code involved

- `tcm/ClusterMetadata.java:103,106` — `EMPTY_METADATA_IDENTIFIER`, `metadataIdentifier`.
- `tcm/CMSOperations.java:232` — `info.put(CMS_ID, Integer.toString(metadata.metadataIdentifier))`.
- `tcm/Startup.java` — startup mode selection, including `Vote`.
- `tcm/discovery/Discovery.java` — peer discovery and the vote to establish a CMS.
- `tcm/transformations/cms/PreInitialize.java` — the entry that creates the initial CMS.
- `tcm/CMSLookup.java` — address rediscovery, added by `eb95b34199`.

## What goes wrong if violated

Two independent linearized histories in one cluster. Each half accepts DDL and ownership
changes the other never sees, and neither is behind the other in any comparable sense, so
no catch-up mechanism engages. Every safety property in the catalog is simultaneously
false. There is no recovery short of choosing a winner and discarding the loser's history.

## Why the partition-during-startup shape matters

The dangerous window is narrow and specific: nodes that have not yet learned of an existing
CMS, partitioned from the ones that have. `Startup`'s `Vote` mode is designed for exactly
"discover an existing CMS or, failing that, participate in a vote to establish a new one."
The failure mode is a node that concludes "failing that" while a CMS exists on the other
side of a partition. Antithesis can hold that partition precisely across the decision
point, which no conventional test does — dtests either have a CMS or do not.

`eb95b34199`'s scenario is adjacent and also worth reaching: a CMS majority restarted with
new broadcast addresses simultaneously. Those nodes cannot find each other, which is a
*liveness* failure the commit fixed — but the temptation in fixing such a thing is a
fallback that forms a fresh service, which would be this property's violation.

## Implementation notes

- Guard on `EPOCH >= 1`. Before initialisation, `CMS_ID` is legitimately `0` on every node
  and `MEMBERS` is legitimately empty; asserting there would fire on every clean startup.
- Assert both cardinality-1 *and* non-zero. Cardinality alone passes vacuously when every
  node reports `0`.
- Only compare across nodes the checker actually reached this cycle. An unreachable node
  contributes no evidence; treating "no answer" as agreement is wrong, and treating it as
  disagreement makes every partition a failure.

## Investigation Log

#### Where is `metadataIdentifier` generated, and can concurrent initialisations produce two values?

- Examined: `ClusterMetadata.java` field declarations and `EMPTY_METADATA_IDENTIFIER`;
  `CMSOperations.describeCMS()` in full; the `transformations/cms/` directory listing
  (`PreInitialize.java`, `PrepareCMSReconfiguration.java`, and siblings);
  `CMSOperationsMBean.initializeCMS(List<String> ignore)` / `abortInitialization(String initiator)`.
- Found: the identifier is a first-class immutable field on `ClusterMetadata`, exposed for
  operator inspection, with a distinguished zero sentinel. `initializeCMS` takes an
  `ignore` list, implying initialisation proceeds without unanimous participation — which
  is precisely the condition under which two initialisations could race.
- Not found: the assignment site. Not in the files read; likely in `PreInitialize` or in
  the `Startup`/`Discovery` vote result handling.
- Conclusion: tagged `(partial)`. The property and its check do not depend on the answer —
  "all nodes agree on one non-zero value" is checkable either way. The answer matters for
  *triage*: if the identifier is derived deterministically (e.g. from the initiator's node
  ID), a mismatch localises immediately to two initiators; if random, it only proves two
  initialisations occurred. Worth resolving before the first real triage, not before
  implementation.
