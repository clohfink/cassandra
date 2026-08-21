# d-schema-agreement-at-same-epoch

## What led to this property

Moving schema into the metadata log was one of TCM's main motivations — eliminating the
schema-disagreement class of bug that gossip-propagated schema produced for a decade. From
`TransactionalClusterMetadata.md`:

> `DistributedSchema` is a component of `ClusterMetadata` and all DDL updates are applied by
> the CMS inserting a log entry containing a schema transformation. As the log entries are
> disseminated around the cluster, each peer applies the transformation to its local
> `ClusterMetadata`, enacting the schema change.

If `ClusterMetadata` is a deterministic function of the log prefix, then same epoch implies
same schema, with no timing excuse available. That makes this a sharp, cheap check on the
core claim.

The reason it is not merely a restatement of `a-log-prefix-agreement` is the *enactment* half:
agreeing on the log entry is necessary but not sufficient. The entry must also be applied to
local database objects — `Keyspace`, `ColumnFamilyStore`, prepared statements — and that
application is where the observed bugs are.

## Code involved

- `tcm/ClusterMetadata.java:111` — `public final DistributedSchema schema`.
- `tcm/transformations/AlterSchema.java` — the DDL transformation.
- `tcm/log/LocalLog.java:646,652` — `notifyPreCommit(before, after, fromSnapshot)` and
  `notifyPostCommit(...)`. Pre-commit fires *before* the `committed.compareAndSet` at 555;
  post-commit after. Anything that must be visible at the same instant as the epoch has to
  be in pre-commit, and anything in post-commit lags the published epoch by however long the
  listener takes.
- `tcm/listeners/` — the `ChangeListener` implementations that initialise db objects.
- `StorageServiceMBean.getSchemaVersion()` — the observation point.

## The bug history that makes the pre/post split the thing to watch

- `9bf1680b1f` "Avoid prepared statement invalidation race when committing schema changes"
  (CASSANDRA-20116).
- `1a6b8e0628` "Invalidate affected prepared stmts on every table metadata change."
- `740879d5a0` "Don't clear prepared statement cache on nodetool cms initialize."

Three commits about prepared-statement invalidation relative to schema commit. All three are
about *when* a side effect of a schema change runs relative to the epoch becoming visible.
That is exactly the seam this property probes, from the outside.

## What goes wrong if violated

Two nodes at the same epoch with different schema means one of them is serving queries
against a table definition the cluster has moved past — wrong column set, wrong types,
stale prepared statements. Because both report the same epoch, TCM's divergence detection is
blind: neither is behind, so no catch-up is triggered and no `CoordinatorBehindException` is
thrown. It is a silent wrong-answer state, and it is the specific state TCM was built to make
impossible.

## Implementation notes

- Group nodes by their reported `EPOCH` and check within each group. Do not compare across
  epochs — nodes at different epochs are *supposed* to have different schema.
- Only groups of size >= 2 produce evidence. On a healthy cluster all nodes are usually in
  one group, which is the strongest case; during churn the groups fragment and each is
  checked independently.
- Sampling epoch and schema version is inherently non-atomic: read epoch, then schema, and
  the node may advance between the two. Read schema version first, then epoch, then schema
  version again, and only assert if both schema reads agree — otherwise the sample straddled
  an enactment and must be discarded. Without this the check produces occasional false
  failures that will get it disabled.

## Investigation Log

#### Is `getSchemaVersion()` computed synchronously with epoch publication, or by a listener that may lag?

- Examined: `ClusterMetadata.java` field list (`schema` is an immutable component, so it is
  *conceptually* atomic with the epoch); `LocalLog.processPendingInternal` lines 548–569 in
  full, establishing the exact ordering `storage.append` → `notifyPreCommit` →
  `committed.compareAndSet` → `maybeNotifyListeners` → `notifyPostCommit`;
  `TransactionalClusterMetadata.md`'s schema section, which notes the change "entails some
  changes to the way the database objects represented in schema are intialised locally."
- Found: the `ClusterMetadata` object itself carries schema atomically with epoch — so if
  `getSchemaVersion()` derives from `ClusterMetadata.current().schema`, the property holds by
  construction and is a strong, cheap check. But `notifyPostCommit` runs *after* the CAS,
  so any listener-maintained view of schema legitimately lags the published epoch.
- Not found: whether `getSchemaVersion()` reads `ClusterMetadata.current().schema` or a
  `Schema.instance`-style cached value maintained by a post-commit listener.
- Conclusion: tagged `(partial)`. This determines whether the property is strong or needs a
  tolerance window, and a tolerance window would substantially weaken it — so it is worth
  resolving before implementation rather than after. The double-read sampling protocol above
  is a partial mitigation either way. If it turns out to be listener-maintained, the better
  property is a SUT-side `Assert.always` in `notifyPostCommit` checking that the listener's
  view matches `after.schema`, which converts an external timing question into an internal
  invariant.
