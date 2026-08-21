#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Renders cassandra.yaml from the environment, then hands off to node-agent.py which owns the
# Cassandra process for the lifetime of the container.

set -euo pipefail

CASSANDRA_HOME="${CASSANDRA_HOME:-/opt/cassandra}"
CONF="${CASSANDRA_HOME}/conf/cassandra.yaml"

# Note: no apostrophes or parentheses in these messages. Inside ${VAR:?word} the word is still
# subject to shell quoting, so an apostrophe opens a quote that runs to the next one -- which
# silently swallows the rest of the script and produces a syntax error hundreds of lines away.
: "${CASSANDRA_LISTEN_ADDRESS:?CASSANDRA_LISTEN_ADDRESS is required and must be this node hostname}"
: "${CASSANDRA_SEEDS:?CASSANDRA_SEEDS is required}"
CASSANDRA_CLUSTER_NAME="${CASSANDRA_CLUSTER_NAME:-antithesis-tcm}"
CASSANDRA_NUM_TOKENS="${CASSANDRA_NUM_TOKENS:-4}"

echo "[entrypoint] configuring ${CASSANDRA_LISTEN_ADDRESS} (seeds=${CASSANDRA_SEEDS})"

for d in "${CASSANDRA_DATA_DIR:-/var/lib/cassandra/data}" \
         "${CASSANDRA_COMMITLOG_DIR:-/var/lib/cassandra/commitlog}" \
         "${CASSANDRA_HINTS_DIR:-/var/lib/cassandra/hints}" \
         "${CASSANDRA_SAVED_CACHES_DIR:-/var/lib/cassandra/saved_caches}" \
         "${CASSANDRA_ACCORD_JOURNAL_DIR:-/var/lib/cassandra/accord_journal}" \
         "${CASSANDRA_HOME}/logs"; do
  mkdir -p "$d"
done

# Render config with Python rather than sed. The keys below live at different indentation levels
# and one (seeds) is nested inside a list-of-maps, which sed handles badly enough to be a real
# source of silent misconfiguration -- and a misconfigured node would fail to form a cluster in a
# way that looks like a TCM bug.
python3 - "$CONF" <<'PYEOF'
import os, re, sys

path = sys.argv[1]
with open(path) as fh:
    lines = fh.readlines()

listen = os.environ["CASSANDRA_LISTEN_ADDRESS"]
seeds = os.environ["CASSANDRA_SEEDS"]

# Top-level scalar settings. Values chosen in scratchbook/deployment-topology.md under
# "Configuration pinned for property soundness" -- changing them changes what properties mean.
scalars = {
    "cluster_name": "'%s'" % os.environ.get("CASSANDRA_CLUSTER_NAME", "antithesis-tcm"),
    "num_tokens": os.environ.get("CASSANDRA_NUM_TOKENS", "4"),
    "listen_address": listen,
    "rpc_address": "0.0.0.0",
    "broadcast_address": listen,
    "broadcast_rpc_address": listen,
    "endpoint_snitch": "SimpleSnitch",
    "auto_snapshot": "false",
    # progress_barrier_default_consistency_level defaults to EACH_QUORUM and
    # progress_barrier_min_consistency_level also defaults to EACH_QUORUM (Config.java:1583-1584),
    # which leaves NO relaxation range -- r-progress-barrier-relaxed could never fire. Setting the
    # minimum one step lower gives exactly one relaxation step, which keeps
    # b-progress-barrier-quorum-sound interpretable (a sub-quorum barrier is unambiguously a defect)
    # while still letting relaxation occur at all.
    "progress_barrier_default_consistency_level": "EACH_QUORUM",
    "progress_barrier_min_consistency_level": "QUORUM",
    # Default is 3600000ms (1 hour). A stuck sequence would consume an entire timeline.
    "progress_barrier_timeout": os.environ.get("CASSANDRA_PROGRESS_BARRIER_TIMEOUT", "60000ms"),
    "progress_barrier_backoff": "1000ms",
    # Off-heap footprint. Five JVMs share one machine, and the defaults are sized for a dedicated
    # host: the chunk cache alone defaults to 512MiB and networking to 128MiB *per node*, which is
    # 3.2GB of off-heap across the cluster before any heap. Left at the defaults the nodes are
    # SIGKILLed by the OOM killer, which presents as "the node never came up" rather than as memory
    # pressure. Heap itself is capped via MAX_HEAP_SIZE in docker-compose.yaml.
    "file_cache_size": os.environ.get("CASSANDRA_FILE_CACHE_SIZE", "32MiB"),
    "networking_cache_size": os.environ.get("CASSANDRA_NETWORKING_CACHE_SIZE", "32MiB"),
    "memtable_heap_space": os.environ.get("CASSANDRA_MEMTABLE_HEAP_SPACE", "64MiB"),
    # Fewer request threads means fewer thread stacks; this harness drives a trickle of traffic, not
    # a benchmark.
    "concurrent_reads": "8",
    "concurrent_writes": "8",
    "concurrent_counter_writes": "8",
}

seen = set()
out = []
for line in lines:
    m = re.match(r"^([a-z_]+):", line)
    if m and m.group(1) in scalars:
        key = m.group(1)
        out.append("%s: %s\n" % (key, scalars[key]))
        seen.add(key)
        continue
    # The seeds entry is nested under seed_provider -> parameters as "- seeds: ...".
    if re.match(r"^\s*- seeds:", line):
        out.append('      - seeds: "%s"\n' % seeds)
        continue
    # Disable deterministic token allocation. conf/cassandra.yaml ships
    # `allocate_tokens_for_local_replication_factor: 3`, which makes bootstrap pick optimal tokens
    # deterministically -- so two spares bootstrapping at once compute the SAME tokens and TCM
    # rejects the second ("some tokens are already assigned"). That made concurrent joins impossible
    # and left r-concurrent-multistep-operations unreached. Commenting it out reverts to random
    # token allocation, so two simultaneous joins get disjoint tokens and are both admitted. Random
    # allocation's mild imbalance is irrelevant to a metadata test.
    if re.match(r"^\s*allocate_tokens_for_local_replication_factor:", line):
        out.append("# disabled by the Antithesis TCM harness (random allocation enables concurrent joins)\n")
        out.append("# " + line if not line.startswith("#") else line)
        continue
    out.append(line)

# Append any pinned setting the shipped cassandra.yaml does not mention. The progress_barrier_*
# keys are commented out or absent in conf/cassandra.yaml even though Config.java defines them,
# so without this they would silently keep their defaults -- and the default min == default would
# disable relaxation entirely.
missing = [k for k in scalars if k not in seen]
if missing:
    out.append("\n# --- pinned by the Antithesis TCM harness ---\n")
    for k in sorted(missing):
        out.append("%s: %s\n" % (k, scalars[k]))

with open(path, "w") as fh:
    fh.writelines(out)

print("[entrypoint] wrote %s (set: %s; appended: %s)"
      % (path, ",".join(sorted(seen)), ",".join(sorted(missing))))
PYEOF

# Point storage at the writable volume paths. cassandra.yaml's directory settings are lists, so
# they are rewritten wholesale rather than patched line-by-line.
python3 - "$CONF" <<'PYEOF'
import os, re, sys

path = sys.argv[1]
with open(path) as fh:
    text = fh.read()

data = os.environ.get("CASSANDRA_DATA_DIR", "/var/lib/cassandra/data")
blocks = {
    "commitlog_directory": os.environ.get("CASSANDRA_COMMITLOG_DIR", "/var/lib/cassandra/commitlog"),
    "hints_directory": os.environ.get("CASSANDRA_HINTS_DIR", "/var/lib/cassandra/hints"),
    "saved_caches_directory": os.environ.get("CASSANDRA_SAVED_CACHES_DIR", "/var/lib/cassandra/saved_caches"),
}

for key, value in blocks.items():
    if re.search(r"(?m)^%s:" % key, text):
        text = re.sub(r"(?m)^%s:.*$" % key, "%s: %s" % (key, value), text)
    else:
        text += "\n%s: %s\n" % (key, value)

# data_file_directories is a YAML list; replace the whole block including its items.
if re.search(r"(?m)^data_file_directories:", text):
    text = re.sub(r"(?m)^data_file_directories:.*(?:\n[ \t]+-.*)*",
                  "data_file_directories:\n    - %s" % data, text)
else:
    text += "\ndata_file_directories:\n    - %s\n" % data

# accord.journal_directory is a *nested* key, so the top-level rewriter above misses it. It must be
# set explicitly: DatabaseDescriptor.createAllDirectories() creates it unconditionally at startup
# (DatabaseDescriptor.java:2407), and when unset it defaults to storagedirFor("accord_journal") ->
# $cassandra.storagedir/accord_journal. That cannot be redirected with -Dcassandra.storagedir,
# because bin/cassandra:202 appends its own -Dcassandra.storagedir=$CASSANDRA_HOME/data *after*
# $JVM_OPTS on the command line (bin/cassandra:215-217) and the last -D wins.
#
# Accord itself is out of scope for this harness -- no Accord-enabled tables are created -- but its
# journal directory is created regardless, so it has to be somewhere writable.
#
# DatabaseDescriptor also rejects this being equal to any data_file_directories entry, the
# commitlog_directory, hints_directory, or local_system_data_file_directory, so it gets its own path.
accord_dir = os.environ.get("CASSANDRA_ACCORD_JOURNAL_DIR", "/var/lib/cassandra/accord_journal")
if re.search(r"(?m)^accord:", text):
    # An uncommented accord block already exists: set or replace journal_directory inside it rather
    # than appending a second `accord:` key, which would be a duplicate YAML mapping key.
    if re.search(r"(?m)^\s+journal_directory:", text):
        text = re.sub(r"(?m)^(\s+)journal_directory:.*$",
                      lambda m: "%sjournal_directory: %s" % (m.group(1), accord_dir), text)
    else:
        text = re.sub(r"(?m)^accord:$", "accord:\n    journal_directory: %s" % accord_dir, text)
else:
    text += "\naccord:\n    journal_directory: %s\n" % accord_dir

with open(path, "w") as fh:
    fh.write(text)
print("[entrypoint] storage directories set under %s (accord journal: %s)"
      % (os.path.dirname(data), accord_dir))
PYEOF

# Remote JMX. The workload reads TCM state through CMSOperationsMBean and StorageServiceMBean, so
# JMX must be reachable from another container -- LOCAL_JMX=yes (the default in
# conf/cassandra-env.sh:243) binds it to localhost only. Auth and TLS are off: this is a hermetic
# simulation network with no untrusted parties, and credentials would only add a failure mode that
# looks like a partition.
#
# Two wrinkles in conf/cassandra-env.sh's configure_jmx() that this has to work around:
#
#   1. The LOCAL_JMX=no branch sets -Dcom.sun.management.jmxremote.authenticate=true. JVM_EXTRA_OPTS
#      is appended to JVM_OPTS afterwards (cassandra-env.sh:333) and duplicate -D properties are
#      last-wins, so the authenticate=false below overrides it.
#   2. It sets -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password
#      *unconditionally*, outside the if/else. With authenticate=false the JMX agent never reads it,
#      but an unreadable path is a startup failure mode that would present as "node never came up",
#      so the Dockerfile creates an empty one. It is created there rather than here because the
#      container runs as the non-root `cassandra` user, which cannot write to /etc.
export LOCAL_JMX=no

# cassandra.in.sh hardcodes cassandra_storagedir="$CASSANDRA_HOME/data" and bin/cassandra passes it
# as -Dcassandra.storagedir. That is the fallback base for every directory setting *not* present in
# cassandra.yaml, and something under it gets created eagerly at startup -- which fails with
# AccessDeniedException because /opt/cassandra is root-owned while the process runs as `cassandra`.
# Overriding it here (last -D wins) points every unset directory at the writable volume, so this does
# not depend on having enumerated each directory setting above correctly.
# -Dcassandra.antithesis.serialization_check=true turns on the per-epoch ClusterMetadata
# serialization round-trip assertion in LocalLog (Antithesis property
# a-metadata-serialization-round-trips). It is gated by this flag because serializing the full
# metadata on every committed epoch is pure overhead in a production node; here it is exactly the
# kind of cheap-in-a-sim, high-signal check we want on.
export JVM_EXTRA_OPTS="${JVM_EXTRA_OPTS:-} \
-Dcassandra.storagedir=/var/lib/cassandra \
-Dcassandra.antithesis.serialization_check=true \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Djava.rmi.server.hostname=${CASSANDRA_LISTEN_ADDRESS} \
-Dcassandra.jmx.remote.port=${JMX_PORT:-7199} \
-Dcom.sun.management.jmxremote.rmi.port=${JMX_PORT:-7199}"

# Coverage instrumentation requires the application jar to be found via a classpath directory, and
# Antithesis injects its instrumentation jars alongside that jar. bin/cassandra.in.sh only adds the
# single build/apache-cassandra*.jar by name, so the injected jars would otherwise be left off the
# classpath. EXTRA_CLASSPATH is appended verbatim (cassandra.in.sh:62), and Java expands the `*`
# wildcard to every jar in the directory.
export EXTRA_CLASSPATH="${CASSANDRA_HOME}/build/*"

echo "[entrypoint] handing off to node-agent.py"
exec python3 /opt/antithesis/node-agent.py
