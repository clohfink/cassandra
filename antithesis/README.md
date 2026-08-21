# Running Apache Cassandra under Antithesis

This directory is a complete Antithesis harness for Cassandra, with the first test template
targeting **Transactional Cluster Metadata (CEP-21)**. It is self-contained: Cassandra's own build
and packaging do not read anything here.

Antithesis runs your system in a deterministic simulation and injects faults — network partitions,
congestion, thread pauses — while your workload exercises it and checks properties. Failures are
reproducible, which is what makes it useful for a subsystem like TCM whose correctness arguments are
about timing.

## What is being tested, and why it was chosen

The property catalog lives in `scratchbook/property-catalog.md`: 29 properties with a per-property
evidence file under `scratchbook/properties/`. It is worth reading before the code, because the
harness's shape follows from it.

Three CEP-21 claims are the reason this harness exists. Each is a *timing* claim asserted in prose
with no mechanical check in-tree — CEP-21 itself notes that a TLA+ spec of epoch visibility "was
explored but omitted":

| Claim (CEP-21) | Property |
|---|---|
| "divergence cannot grow larger than a single epoch, so any two read or write quorums will have overlap" | `c-cms-reconfiguration-quorum-overlap` |
| a lagging coordinator "will **not** be able to collect a quorum for read or write that is inconsistent with a quorum obtained using metadata that is up to date" | `b-progress-barrier-quorum-sound` |
| "We make *no assumptions* about liveness of the node between execution of in-progress sequence steps" | `b-sequence-resumable-after-crash` |

The rest of the catalog is either derived from those, or a regression target for a specific fixed
bug (CASSANDRA-21455 log gaps, CASSANDRA-19384 replay exposing intermediate state, CASSANDRA-19872
CMS membership changing under a commit, CASSANDRA-19782 peers tables after replacement,
CASSANDRA-20116 prepared-statement invalidation, and others — each cited in its evidence file).

## Layout

```
antithesis/
  scratchbook/          research: SUT analysis, property catalog, evidence, topology, evaluation
  config/
    docker-compose.yaml the topology Antithesis brings up (this is the "config image" content)
  docker/
    cassandra.Dockerfile     node image; builds Cassandra with the SDK on the compile classpath
    workload.Dockerfile      workload image
    entrypoint-cassandra.sh  renders cassandra.yaml, then hands off to the control agent
    node-agent.py            supervisor + HTTP control agent (start/stop/restart/wipe/replace)
    entrypoint-workload.sh   waits for the cluster, emits setup_complete, idles
  workload/             Java workload driver (Ant, standalone from Cassandra's build)
  test/v1/tcm/          the test template: the executable test commands
  build-images.sh       builds both images and checks they are amd64
  setup-complete.sh     emits the signal that starts fault injection
```

## Local resource requirements

The full topology is **10 Cassandra JVMs (7 ring + 3 spares) plus the workload**, sized for the
`cassandra_oss` Antithesis runner so two joins can land on disjoint token arcs and make
`r-concurrent-multistep-operations` reachable (see `scratchbook/deployment-topology.md`). Ten JVMs
do **not** fit a small local Docker VM. Measured on a 7.65 GiB Docker VM (Apple silicon, amd64 under
emulation), even at `CASSANDRA_MAX_HEAP=256M` only ~5 of 10 nodes reach healthy before a seed fails
its healthcheck, so `snouty validate` cannot bring the full topology up locally there. This is a
local-machine constraint, not an Antithesis one — Antithesis instances are far larger, which is where
the full 10-node run belongs.

The failure mode when memory is short is misleading: a node is SIGKILLed by the OOM killer, the
control agent reports `last_exit_code: -9`, and the node simply stops being part of the cluster. If
that node is a seed or the sole metadata-service member, it blocks every metadata commit — which
surfaces as `GivingUpException: Could not succeed sending TCM_COMMIT_REQ` from an unrelated
`CREATE KEYSPACE`. (The workload's `setup-cms` step grows CMS to RF=3 to remove that single point of
failure once enough nodes are up.)

Heap is capped at 384 MB per node (`MAX_HEAP_SIZE`, override with the env var), and the off-heap
caches are capped in the entrypoint, because Cassandra's defaults are sized for a dedicated host:
unset, each node takes half of visible RAM for heap plus ~640 MB of off-heap caches.

**Locally, validate a subset instead of the full ring** — the images, entrypoint, and config are
identical, so a 3-node bring-up confirms they are sound before launching the full topology to
Antithesis:

```bash
# smaller heaps for whatever subset you run
export CASSANDRA_MAX_HEAP=256M CASSANDRA_HEAP_NEW=64M

# three ring nodes: confirms images/entrypoint/config boot and form a cluster + CMS
# (no spares means no joins and no concurrent movements — those properties will not fire)
docker compose up -d cassandra-1 cassandra-2 cassandra-3
CASSANDRA_NODES=cassandra-1,cassandra-2,cassandra-3 docker compose up -d workload
```

The full 10-node `snouty validate`/launch is expected to run where there is more memory (the
`cassandra_oss` runner, or a Docker VM raised past ~10 GB via Docker Desktop → Settings → Resources).

## Local workflow

Nothing here submits a run. Launching is a separate, deliberate step (see "Submitting a run").

```bash
# 0. a container runtime must be running; snouty validate inspects the built images
open -a Docker          # macOS

# 1. build both images
antithesis/build-images.sh

# 2. validate the config. This is NOT static: it brings the whole stack up, waits for
#    setup_complete, then discovers and validates the test template inside the running containers.
#    The default --timeout is 60s, which a five-node Cassandra cluster will not meet -- and on
#    Apple silicon, where the amd64 images run under emulation, startup is several minutes.
snouty validate antithesis/config --timeout 1500

# 3. bring the cluster up locally
cd antithesis/config && docker compose up
```

The workload container waits for all five nodes to answer CQL *and* JMX, creates the probe schema,
then emits `setup_complete` and idles. Outside Antithesis nothing schedules test commands, so run
them by hand:

```bash
cd antithesis/config
docker compose exec workload /opt/antithesis/test/v1/tcm/anytime_check_tcm_invariants
docker compose exec workload /opt/antithesis/test/v1/tcm/serial_driver_cms_churn
docker compose exec workload /opt/antithesis/test/v1/tcm/serial_driver_membership_churn
docker compose exec workload /opt/antithesis/test/v1/tcm/eventually_check_tcm_recovery
```

### Seeing assertion results locally

The SDK is a no-op outside Antithesis unless you point it at a file. Then it writes exactly the
JSONL it would send to the platform, which is the fastest way to confirm an assertion is wired up
and its `details` payload is useful:

```bash
docker compose exec -e ANTITHESIS_SDK_LOCAL_OUTPUT=/tmp/sdk.jsonl workload \
    /opt/antithesis/test/v1/tcm/anytime_check_tcm_invariants
docker compose exec workload cat /tmp/sdk.jsonl
```

This works for the Cassandra-side assertions too — set `ANTITHESIS_SDK_LOCAL_OUTPUT` on a
`cassandra-*` service in `docker-compose.yaml`.

### Driving nodes directly

Each node runs a control agent on port 7788. It exists because Antithesis node-termination faults
are **off by default**, and because node replacement needs a JVM flag that only a process restart
can apply (`-Dcassandra.replace_address_first_boot`). It is a workload action, not a fault bypass:
it is reachable only over the container network, so partitions apply to it too.

```bash
docker compose exec workload curl -s http://cassandra-4:7788/status
docker compose exec workload curl -sX POST http://cassandra-4:7788/restart
docker compose exec workload curl -sX POST 'http://cassandra-4:7788/stop?force=1'
docker compose exec workload curl -sX POST 'http://cassandra-4:7788/replace?address=cassandra-3'
docker compose exec workload curl -sX POST http://cassandra-4:7788/wipe-and-restart
```

## Adding a property

1. Catalogue it in `scratchbook/property-catalog.md` and write
   `scratchbook/properties/<slug>.md`. State which assertion type and *why* — the choice is a
   commitment, and `Sometimes` on a condition that is structurally always true is really a
   `Reachable`.
2. Implement it:
   - workload-side → a method in `workload/src/main/java/.../Checks.java`, called from
     `Main.checkInvariants`;
   - Cassandra-side → an `Assert.*` call in `src/java/org/apache/cassandra/tcm/`, with the details
     payload built by `AntithesisDetails.of(...)`.
3. Keep the message a **string literal** at the call site. Each distinct message becomes its own
   test property, and the cataloger requires a compile-time constant — which is why
   `AntithesisDetails` shares only the payload construction and never wraps the assertion call.
4. If it is a safety property whose evidence comes from more than one node, make sure it only
   evaluates over nodes that answered, and that it counts them (see `h-all-nodes-compared`).

## Bring-up findings

Five things had to be discovered by actually running this. They are all encoded in the harness now,
but they are the sort of thing that is expensive to rediscover, and each one presented as
"the node never came up" rather than as its actual cause.

1. **`bin/cassandra` refuses to run as root** ("Running Cassandra as root user or group is not
   recommended"), exiting 1. The image therefore creates a `cassandra` user (uid 999) and chowns
   `/var/lib/cassandra`, `/opt/cassandra/{conf,logs,build,data}`, and `/etc/cassandra`. `conf` is in
   that list because the entrypoint rewrites `cassandra.yaml` in place; `build` because that is
   where Antithesis injects instrumentation jars.
2. **`-Dcassandra.storagedir` cannot be overridden via `JVM_EXTRA_OPTS`.** `bin/cassandra:202`
   appends its own `-Dcassandra.storagedir=$CASSANDRA_HOME/data` *after* `$JVM_OPTS` on the command
   line (`bin/cassandra:215-217`), and the last `-D` wins. Anything relying on storagedir therefore
   resolves under `/opt/cassandra/data`, which is why that path is created and chowned as a
   safety net.
3. **`accord.journal_directory` is a nested YAML key and must be set explicitly.**
   `DatabaseDescriptor.createAllDirectories()` creates it unconditionally at startup, and unset it
   defaults to `storagedirFor("accord_journal")` — see (2). Accord is out of scope for this harness,
   but its journal directory is still created. `DatabaseDescriptor` also rejects it being equal to
   any data directory, the commitlog, hints, or local system data directory, so it gets its own path.
4. **`metadata_directory` is not a real setting.** TCM persists its log in the
   `system_cluster_metadata` keyspace, under the data directories — which is what makes the control
   agent's `wipe-and-restart` genuinely force a node back through `Startup`/`Discovery`.
5. **`LOCAL_JMX=no` sets a JMX password file path unconditionally**, outside the if/else in
   `conf/cassandra-env.sh`. With `authenticate=false` the agent never reads it, but a non-existent
   path is a startup failure, so the image creates an empty one.

A sixth was a bug in this harness rather than an integration detail, and is worth repeating because
it is easy to write again: inside `${VAR:?message}`, **shell quoting still applies**. An apostrophe
in the message (`(this node's hostname)`) opened a single quote that swallowed the remaining ~100
lines of the script and produced a syntax error far from its cause. `bash -n` on every script is
cheap and catches it:

```bash
for f in $(find antithesis -name '*.sh') antithesis/test/v1/tcm/*; do bash -n "$f" || echo "FAIL $f"; done
python3 -m py_compile antithesis/docker/node-agent.py
```

## Two things that will bite you

**Assertion cataloging is mandatory.** SDK assertions only work if the code containing them is under
`/opt/antithesis/catalog/` in the image. Both Dockerfiles do this with a single symlink (Antithesis
follows exactly one symlink level and must not meet a symlink-to-a-symlink). The same mechanism
enables coverage instrumentation, which is in turn what enables **thread-pausing faults** — the
fault type that reaches the `LocalLog` publication CAS window.

**Never launch via `java -jar`.** Antithesis injects instrumentation jars alongside the application
jar, and `-jar` ignores `-cp`, so the process would run uninstrumented and silently untested.
Cassandra's `bin/cassandra.in.sh` builds a classpath, which is compatible — but it adds only the
single `build/apache-cassandra*.jar` by name, so `entrypoint-cassandra.sh` sets
`EXTRA_CLASSPATH=$CASSANDRA_HOME/build/*` to pick up the injected jars. `helper_run.sh` does the
equivalent for the workload.

## The Antithesis SDK dependency

The Cassandra-side assertions need `com.antithesis:sdk` (plus its `com.antithesis:ffi` companion).
Per the repo-root `AGENTS.md` this needs OSS community approval, so it is kept deliberately small
and separable — see the "one new dependency" section of `AGENTS.md` in this directory for the full
rationale. In short: nothing is committed to `lib/`, no edit to `build.xml`, the jar is fetched at
image build time into `build/lib/jars/` (which is already on `cassandra.classpath`), every call site
is a no-op outside Antithesis, and the existing Java `assert` statements are left in place rather
than replaced.

The workload-side properties — about two thirds of the catalog — need no Cassandra source changes at
all, so the harness is still useful if the dependency is declined.

## Submitting a run

Use the `antithesis-launch` skill rather than calling `snouty launch` yourself; it discovers the
harness layout, validates, and bails before submitting if validation fails. Submission needs a
tenant and credentials, and it pushes images to the Antithesis registry — so it is an outward-facing
action that should be an explicit decision, not a side effect of a build.

### Everything must be amd64 — including the config image `snouty` builds for you

All three images the platform pulls must be `linux/amd64`: the two service images *and* the
`snouty-config` image that `snouty launch --config` builds and pushes at submit time. The service
images are handled by `platform: linux/amd64` in `docker-compose.yaml`, but the config image is
built by `snouty` using the plain docker CLI, which on Apple silicon defaults to the **host** arch
(arm64). The platform then rejects the run:

```
Container .../snouty-config has architecture arm64, which is not supported.
[STATUS] {"antithesis_error":{"code":4005,"message":"Unsupported container type"}}
```

**`snouty validate` does not catch this** — it runs the arm64 config image on the arm64 host, where
it works fine. The mismatch only surfaces in the amd64-only Antithesis environment, after the run is
submitted. Our first run (`v59-13`) died exactly here.

The fix is one environment variable, which makes docker (and thus snouty's config-image build)
target amd64:

```bash
export DOCKER_DEFAULT_PLATFORM=linux/amd64
```

Set it before `snouty launch` on any non-amd64 host. Verify a pushed config image with
`docker image inspect <snouty-config:tag> --format '{{.Os}}/{{.Architecture}}'` → must be
`linux/amd64`. (On a native amd64 CI host none of this is needed, which is where launches should
ultimately run.)

## Known gaps

Recorded properly in `scratchbook/property-relationships.md` ("Gaps") and
`scratchbook/evaluation/synthesis.md`. The two that matter most:

- **The catalog tests the control plane, not the data-path guarantee the control plane exists to
  provide.** A TCM implementation could pass all 29 properties while losing acknowledged writes,
  because CEP-21's coordinator-side placement re-check has no property. Closing this needs a
  linearizability workload with a history checker — a different workload shape, and one that
  overlaps Harry and `test/simulator`. Escalated as bias B1; unresolved by design.
- **The gossip→TCM upgrade path is not covered**, and it is the densest area in the bug history.
  It needs a mixed-mode deployment, so it is a second harness rather than an addition to this one.
