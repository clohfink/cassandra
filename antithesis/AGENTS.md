# Antithesis harness for Apache Cassandra

This directory contains everything needed to run Cassandra under
[Antithesis](https://antithesis.com), a deterministic-simulation testing platform. The first
harness targets **Transactional Cluster Metadata (CEP-21)**.

Nothing in here is part of Cassandra's build or its shipped artifacts. `ant jar`, `ant test`, and
the packaging targets do not read this directory.

## Layout

| Path | What it is |
|---|---|
| `scratchbook/` | Research artifacts: SUT analysis, 29-property catalog, per-property evidence, topology plan, evaluation. Read `scratchbook/property-catalog.md` first. |
| `config/docker-compose.yaml` | The topology Antithesis brings up. This is the "config image" content. |
| `docker/` | Dockerfiles, the Cassandra entrypoint/supervisor, and the node control agent. |
| `workload/` | The Java workload driver: actions that churn TCM state and checkers that evaluate properties. Built with Ant. |
| `test/v1/tcm/` | The test template — the executable test commands Antithesis schedules. |
| `setup-complete.sh` | Emits the `antithesis_setup` signal that starts fault injection. |

## Working on this

Skills: `antithesis-research` (analyse and catalogue), `antithesis-setup` (this scaffolding),
`antithesis-workload` (test commands and assertions), `antithesis-triage` (read a finished run),
`antithesis-debug` (multiverse debugger), `antithesis-launch` (submit a run — do not call
`snouty launch` directly).

Local checks, in increasing cost:

```bash
snouty validate antithesis/config           # static validation of the compose config
antithesis/build-images.sh                  # build both images (runs the Cassandra Ant build)
docker compose -f antithesis/config/docker-compose.yaml up      # bring the cluster up locally
docker compose -f antithesis/config/docker-compose.yaml exec workload \
    /opt/antithesis/test/v1/tcm/anytime_check_tcm_invariants     # run one test command by hand
```

`antithesis/README.md` has the full local workflow, including how to exercise individual test
commands and how to read workload output when running outside Antithesis.

## The one new dependency: `com.antithesis:sdk`

`AGENTS.md` at the repo root says dependencies require OSS community approval, and that `lib/` must
not be modified. This harness respects both, and the dependency ask is deliberately kept small and
separable so it can be discussed on its own merits:

- **Nothing is added to `lib/` and nothing is committed.** The SDK jar is fetched from Maven
  Central at *image build time* and placed in `build/lib/jars/` inside the container. Cassandra's
  `cassandra.classpath` in `build.xml` is `<fileset dir="${build.dir.lib}" include="**/*.jar"/>`,
  so the jar lands on the compile classpath with **zero edits to `build.xml` or `lib/`**.
- **No production behaviour changes.** Every SDK method used here (`Assert.always`,
  `Assert.sometimes`, `Assert.unreachable`) is a no-op outside Antithesis unless
  `ANTITHESIS_SDK_LOCAL_OUTPUT` is set, and the SDK's assertions never terminate the process.
- **Call sites are confined to `src/java/org/apache/cassandra/tcm/`** and are additive: existing
  Java `assert` statements are left in place, not replaced, so `-ea` behaviour in unit tests and
  dtests is unchanged.
- **Without it, a whole class of property is unavailable.** Six properties in the catalog can only
  be checked from inside the process — `ProgressBarrier` consistency-level relaxation, the
  `LocalLog` publication CAS, `LockedRanges` admission, and CMS read/write quorum overlap have no
  external observation surface at all. See `scratchbook/evaluation/implementability.md`.

If the dependency is not acceptable, the harness still runs: the workload-side properties (roughly
two thirds of the catalog) need no Cassandra source changes. Reverting the SUT-side half means
dropping the SDK jar step from `docker/cassandra.Dockerfile` and the assertion call sites listed in
`scratchbook/existing-assertions.md`.

## Conventions this harness follows

From the Antithesis Docker best practices and the test-composer reference:

- every service sets `platform: linux/amd64`, `init: true`, and `NO_COLOR=1`
- `hostname` matches `container_name`, and neither contains an underscore
- no custom `logging:` driver, no `internal: true` network, no `pull_policy:`
- `depends_on` uses `condition: service_healthy` against a real `healthcheck`
- `setup_complete` is emitted by the workload container's **entrypoint**, never by a `first_`
  command — `first_` commands only run after Antithesis has already received the signal
- Cassandra is launched via a classpath (`bin/cassandra` builds one), never `java -jar`, because
  Antithesis injects instrumentation jars alongside the application jar and `-jar` ignores `-cp`
- files under `test/v1/tcm/` prefixed `helper_` are ignored by Antithesis and hold shared shell
  functions
- **launch with `DOCKER_DEFAULT_PLATFORM=linux/amd64` on non-amd64 hosts.** `snouty launch --config`
  builds the `snouty-config` image with the plain docker CLI, which on Apple silicon defaults to
  arm64 and the platform rejects it (`error 4005, Unsupported container type`). `snouty validate`
  cannot catch this (arm-on-arm runs locally); it only fails after submission. Tenant/registry for
  launches: `ANTITHESIS_TENANT=crimson-whale`,
  `ANTITHESIS_REPOSITORY=us-central1-docker.pkg.dev/molten-verve-216720/netflix-repository`. See
  `README.md` "Submitting a run".
