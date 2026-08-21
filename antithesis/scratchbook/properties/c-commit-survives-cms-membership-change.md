# c-commit-survives-cms-membership-change

## What led to this property

`6dc9ca99fa`, "Retry if node leaves CMS while committing a transformation"
(CASSANDRA-19872). The bug is in the title: a commit in flight when its serving CMS node
left the CMS. The fix is a retry — and retries against a replicated log are exactly where
exactly-once semantics get lost.

Reinforced by `4f49ca5e29`: "TCM's `Retry.Deadline#retryIndefinitely` is dangerous if used
with `RemoteProcessor` as the deadline does not impact message retries" (CASSANDRA-20059).
A retry policy whose deadline does not bound the underlying message retries can produce
duplicate submissions long after the caller has given up. And `802ce7f8b2` "Always send TCM
commit failures as Messaging failures" plus `e0766e95bc` "cap message sizes for commit
failures" — the failure-reporting path has needed repeated attention, and a
misreported failure is precisely what makes a client retry something that already committed.

## Code involved

- `tcm/RemoteProcessor.java` — the non-CMS node's commit RPC, with retries against
  potentially *different* CMS nodes across attempts. This is the crux: attempt 1 goes to
  node X, attempt 2 to node Y, and Y must decide whether X's attempt landed.
- `tcm/Retry.java` — `Retry.Deadline`, the policy CASSANDRA-20059 was about.
- `tcm/AbstractLocalProcessor.java`, `tcm/PaxosBackedProcessor.java` — the CMS-side append
  and the `Success`/`Reject` decision.
- `tcm/Commit.java` — the request/response types.
- `tcm/CMSLookup.java` — which CMS node a submitter targets;
  `9a896cbaeb` added a "policy for selecting CMS host when submitting commit request"
  (`getCmsCommitMemberPreferencePolicy` / `setCmsCommitMemberPreferencePolicy` on the MBean),
  so target selection is configurable and therefore variable across retries.
- `tcm/transformations/CustomTransformation.java` — the intended tagging vehicle.

Timeout knobs visible on `CMSOperationsMBean`, all relevant to widening the window:
`getCmsAwaitTimeoutMillis`, `getCmsCommitTimeoutMillis`,
`getCmsCommitRetryInitialDelayMillis`, `getCmsCommitRetryMaxDelayMillis`.

## What goes wrong if violated

Two failure directions, both bad and both silent:

- **Lost commit.** The submitter is told the commit failed; it actually appended. The
  operator retries a `DROP TABLE` that already happened, or concludes a decommission did not
  start when it did. TCM_implementation.md notes rejections are "linearized using a read that
  confirms that transformation was executed against the highest epoch" — that read is what
  must distinguish "did not apply" from "applied, response lost."
- **Double commit.** The transformation appends twice at two epochs. For an idempotent
  transformation this is merely confusing; for `PrepareJoin` it means two sequences for one
  node, which `PrepareJoin` is supposed to reject ("If any in-progress sequences associated
  with the current node are present, `PrepareJoin` is rejected") — so a double commit here
  would surface as that rejection failing to fire.

## Why the tagging approach matters

Exactly-once is only checkable if the workload can *identify* its own submissions in the
log. Without unique tags the check degenerates to counting, which cannot distinguish a lost
commit from one the workload never actually sent. `CustomTransformation` is the natural
vehicle — a transformation kind that exists specifically for extension — and the fallback is
DDL with distinctly-named tables, which is heavier (real schema churn) but unambiguous and
always available. Both are recorded because the fallback is what ships if the open question
below resolves badly.

## Implementation notes

- The workload must record "submitted" *before* sending and "acked" on response, and treat
  a timeout as neither — an unknown outcome. Then: every acked tag must appear exactly once;
  every unknown tag must appear zero or one times; no tag may appear twice. Collapsing
  unknown into failed is the classic error that turns this property into a false-positive
  generator under partition.
- Read the log via the same mechanism as `a-log-prefix-agreement`, and share its resolution
  of the `dumpLog` sourcing question.

## Investigation Log

#### Is `CustomTransformation` submittable by a client, and does it round-trip intact?

- Examined: `tcm/transformations/CustomTransformation.java` (exists, is a
  `Transformation.Kind`, and has `assert`/`Invariants` usage per the TCM assert scan);
  `Transformation.java`'s `Kind` enum plumbing; `CMSOperationsMBean` in full for a
  submission entry point; `ClusterMetadataLogTable`'s `kind`/`transformation` columns, which
  render any kind as text and would therefore display a custom tag.
- Found: the kind exists and would be observable in both `cluster_metadata_log` and
  `dumpLog` output, so the *observation* half works. `CMSOperationsMBean` exposes no generic
  "commit this transformation" method — its mutators are all specific operations.
- Not found: any client-reachable submission path for `CustomTransformation`. It may be
  intended purely for in-tree extensions registered at startup rather than for external
  submission.
- Conclusion: tagged `(partial)`. The fallback is not merely acceptable but arguably better:
  DDL is a real user-facing operation, so tagging via uniquely-named tables tests the path
  operators actually use. Implementation proceeds with DDL tagging; `CustomTransformation`
  stays noted as a lighter-weight option if a submission path turns out to exist.
