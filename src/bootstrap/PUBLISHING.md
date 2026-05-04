# Building and Publishing a Snapshot jvm-dtest JAR

This repository ships two artifacts to Artifactory that let consumers run
Cassandra in-JVM distributed tests (jvm-dtest) outside this repo:

| Coordinate | Purpose |
| --- | --- |
| `com.netflix.cde:nfcassandra-jvm-dtest:<version>` | Shaded fat JAR — Cassandra classes + bundled dependencies |
| `com.netflix.cde:nfcassandra-jvm-dtest-bootstrap:<version>` | Gradle plugin + `DtestClusterFactory` that handles classloader isolation for consumers |

CI publishes a new build of both artifacts on every successful run of the
[`CDE-nfcassandra-build-artifacts`](http://cde.builds.test.netflix.net/view/CDE/job/CDE-nfcassandra-build-artifacts)
Jenkins job, using the version `4.1.8.<BUILD_NUMBER>`.

When you're iterating on a jvm-dtest change locally and want consumers to test
it before the change lands on `cassandra-4.1`, you can build and publish a
snapshot from your laptop. This guide covers that workflow.

## Version scheme

`build.gradle` derives the artifact version from the `BUILD_NUMBER` environment
variable:

```groovy
"${ant.properties['base.version']}.${System.getenv('BUILD_NUMBER') ?: 'local'}"
```

So with no environment variable, you'd publish `4.1.8.local`. To avoid clashing
with previous local snapshots (or anyone else's) and to give consumers a
stable, pinnable version, set `BUILD_NUMBER` to a unique suffix like
`local1`, `local2`, etc.

Pick the next free one by checking what's already in Artifactory:

```bash
curl -s "https://artifacts.netflix.com/api/search/artifact?name=nfcassandra-jvm-dtest&repos=libs-snapshots-local" \
  | jq -r '.results[].uri' \
  | grep -E 'local[0-9]+' | sort -V
```

(Or use the Artifactory UI — repository `libs-snapshots-local`, path
`com/netflix/cde/nfcassandra-jvm-dtest/`.)

## Prerequisites

- **JDK 21** on `JAVA_HOME`. JDK 11 may fail to compile some Cassandra source
  files that use post-JDK-11 stream APIs (e.g. `Stream.toList()`). Once the
  Apache toolchain is fully JDK 11 compliant on this branch, you can use JDK 11
  via `-Duse.jdk11=true`, but JDK 21 is the safe default.
- **`ant-optional`** for the `JUnitTask` Ant taskdef:
  ```bash
  sudo apt-get install -y ant-optional   # Debian / Ubuntu
  ```
  This is only needed if you also intend to run `ant testsome` to validate your
  change before publishing — the publish task itself doesn't depend on it.
- **Artifactory credentials** in `~/.gradle/gradle.properties` (the standard
  Nebula setup). If you can already publish from this machine to any internal
  Netflix project, you're set.

## Build and publish

From the repository root, with your branch checked out and your changes
committed (or in the working tree — Gradle picks them up either way):

```bash
BUILD_NUMBER=local3 ./gradlew publishJvmDtest
```

`publishJvmDtest` (defined in `build.gradle`) is a single convenience task that
runs the full chain:

1. **`buildDtestJar`** — forks an Ant subprocess to run `ant dtest-jar`,
   producing `build/dtest-<base.version>.jar`. (We fork rather than reuse the
   imported Ant targets because `ant.importBuild` caches `<condition>` results
   at Gradle configuration time, which breaks after `./gradlew clean` deletes
   `build/`.)
2. **`shadeDtestJar`** — relocates `software.amazon.*` →
   `relocated.software.amazon.*` (excluding `awssdk.crt.**`, which must keep
   its original FQN for JNI symbol resolution), strips `META-INF/*.SF/.RSA/.DSA`
   signatures (invalid after shading), and writes the shaded fat JAR to
   `build/dtest-shaded/`.
3. **`verifyShadedDtestJar`** — fails the build if any AWS SDK classes were
   missed by the shade rule, or if any *relocated* class declares a `native`
   method (which would `UnsatisfiedLinkError` at runtime because the bundled
   `.so`/`.dylib` registers JNI symbols against the *original* class names).
4. **Publish** — uploads both artifacts to `libs-snapshots-local`:
   - `com.netflix.cde:nfcassandra-jvm-dtest:4.1.8.local3`
   - `com.netflix.cde:nfcassandra-jvm-dtest-bootstrap:4.1.8.local3`

## Verify the upload

```bash
VERSION=4.1.8.local3
for art in nfcassandra-jvm-dtest nfcassandra-jvm-dtest-bootstrap; do
  echo "--- $art:$VERSION ---"
  curl -sI "https://artifacts.netflix.com/libs-snapshots-local/com/netflix/cde/$art/$VERSION/$art-$VERSION.jar" \
    | head -1
done
```

Both should return `HTTP/1.1 200 OK`.

## Consume from another project

Set the dependency version to your snapshot:

```groovy
// In the consuming project's build.gradle
def dtestVersion = '4.1.8.local3'

dependencies {
    integTestImplementation "com.netflix.cde:nfcassandra-jvm-dtest-bootstrap:${dtestVersion}"
    integTestCompileOnly    "com.netflix.cde:nfcassandra-jvm-dtest:${dtestVersion}"
}
```

`libs-snapshots-local` is in the default Nebula resolver chain for internal
Netflix projects, so no additional repo declaration is needed.

## Updating an existing snapshot

Snapshot artifacts are immutable in Artifactory — you cannot republish over the
same version. Bump `BUILD_NUMBER` and republish:

```bash
BUILD_NUMBER=local4 ./gradlew publishJvmDtest
```

Then update the consumer to point at `local4`.

## Troubleshooting

- **`error: cannot find symbol method toList()`** — you're on JDK 11. Switch to
  JDK 21 (`JAVA_HOME=/path/to/jdk21`).
- **`bad class file: ... wrong version 65.0, should be 55.0`** — stale class
  files compiled with a different JDK in `build/`. Run `ant realclean` and
  rebuild.
- **`taskdef A class needed by class org.apache.cassandra.JStackJUnitTask
  cannot be found: org/apache/tools/ant/taskdefs/optional/junit/JUnitTask`** —
  install `ant-optional` (see Prerequisites). This only blocks `ant testsome`,
  not `publishJvmDtest`.
- **`401 Unauthorized` from Artifactory** — your `~/.gradle/gradle.properties`
  is missing `artifactoryUser` / `artifactoryPassword`, or the credentials are
  stale. Refresh them via the standard Netflix dev-tools setup.
- **`Found N relocated classes with native methods`** from
  `verifyShadedDtestJar` — the shade rule pulled JNI-bearing classes into the
  `relocated/` namespace. Add an `exclude` to the `relocate` block in
  `build.gradle` for the affected package, similar to the existing
  `software.amazon.awssdk.crt.**` exclusion.

## What CI does differently

The Jenkins job runs the same `./gradlew publish` chain, with `BUILD_NUMBER`
set by Jenkins to a monotonic integer (`911`, `912`, ...) and `ROCKET_TAG` set
on tag builds (which produces a clean `4.1.<N>` version with no build-number
suffix). For non-tag builds, a CI artifact and a local snapshot are
functionally identical — just different version strings.
