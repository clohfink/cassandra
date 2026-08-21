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
# Cassandra node image for the Antithesis TCM harness.
#
# Build context is the repository root:
#   docker build --platform linux/amd64 -f antithesis/docker/cassandra.Dockerfile -t cassandra-antithesis:latest .

# ---------------------------------------------------------------------------
# Builder: compile Cassandra with the Antithesis SDK on the compile classpath
# ---------------------------------------------------------------------------
FROM docker.io/library/eclipse-temurin:17-jdk-jammy AS builder

ARG ANTITHESIS_SDK_VERSION=1.6.0
ARG DEBIAN_FRONTEND=noninteractive
ENV NO_COLOR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        ant ant-optional curl git python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . /src

# Antithesis assertion cataloging is required for SDK assertions to work, and the SDK must be on
# the compile classpath for the TCM call sites to compile.
#
# Nothing is added to lib/ and nothing is committed to the repo. Cassandra's compile path is
#     <path id="cassandra.classpath">
#       <pathelement location="${build.classes.main}"/>
#       <fileset dir="${build.dir.lib}"><include name="**/*.jar"/></fileset>
#     </path>
# (build.xml:467-473), so dropping the jar under build/lib/jars/ puts it on the classpath with zero
# edits to build.xml or lib/. resolver-retrieve-build retrieves *into* that directory rather than
# recreating it (.build/build-resolver.xml:188), so the jar survives dependency resolution -- which
# is why resolution is run first and the jar copied in afterwards.
#
# Two jars are needed, not one: com.antithesis:sdk depends on com.antithesis:ffi (the SDK README
# gives its minimum as "antithesis-ffi 1.4.6 (or above)"). It also needs jackson 2.14.0+, which
# Cassandra already ships in lib/ (jackson-databind 2.19.2) -- the details parameter on every
# assertion is a com.fasterxml.jackson.databind.node.ObjectNode.
# Order matters, and this exact sequence was validated by running it:
#
#   1. realclean -- the build context may carry a stale build/ directory from the developer's
#      working tree. A leftover apache-cassandra-<other-version>.jar there makes
#      bin/cassandra.in.sh abort with "JAR artifacts for multiple versions", and accumulated
#      duplicate dependency versions in build/lib/jars break compilation outright (two jamm
#      versions on the classpath resolve MemoryMeter to the one without ByteBufferMode).
#   2. resolver-retrieve-build -- repopulates build/lib/jars with exactly one version of each
#      dependency.
#   3. stage the SDK jars -- retrieval copies *into* build/lib/jars rather than recreating it
#      (.build/build-resolver.xml:188), so anything added afterwards survives.
#   4. ai-build -- runs `ant clean jar`, and `clean` deletes build/classes, build/test and
#      build/tmp but NOT build/lib (build.xml "clean" target), so the staged SDK jars persist
#      through it.
# realclean is tolerated failing: .dockerignore excludes build/ from the context, so there is
# usually nothing to clean and realclean's fileset over a non-existent build/lib errors. It is kept
# for the case where the image is built with a context that does include build/.
RUN (ant realclean -q || true) \
 && ant resolver-retrieve-build \
 && for artifact in sdk ffi; do \
      curl --fail --location --silent --show-error \
        -o build/lib/jars/antithesis-${artifact}-${ANTITHESIS_SDK_VERSION}.jar \
        https://repo1.maven.org/maven2/com/antithesis/${artifact}/${ANTITHESIS_SDK_VERSION}/${artifact}-${ANTITHESIS_SDK_VERSION}.jar; \
    done \
 && ls -l build/lib/jars/antithesis-*.jar

# Checkstyle is skipped here for image build time only; it is run in the repo by
# `.build/sh/ai-build` without arguments, which is where style violations should be caught.
RUN .build/sh/ai-build --no-checkstyle

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM docker.io/library/eclipse-temurin:17-jdk-jammy

ARG ANTITHESIS_SDK_VERSION=1.6.0
ARG DEBIAN_FRONTEND=noninteractive

ENV NO_COLOR=1 \
    FORCE_COLOR=0 \
    CASSANDRA_HOME=/opt/cassandra \
    CASSANDRA_CONF=/opt/cassandra/conf \
    JMX_PORT=7199 \
    NODE_AGENT_PORT=7788

# python3 for cqlsh, the node control agent, and config rendering. procps/iproute2 are for
# debugging inside the multiverse debugger, where there is no package manager available.
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-venv procps iproute2 iputils-ping curl netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Runtime layout mirrors a source checkout, because bin/cassandra.in.sh derives the classpath from
# $CASSANDRA_HOME/build/apache-cassandra*.jar plus $CASSANDRA_HOME/lib/*.jar.
COPY --from=builder /src/bin        /opt/cassandra/bin
COPY --from=builder /src/conf       /opt/cassandra/conf
COPY --from=builder /src/lib        /opt/cassandra/lib
COPY --from=builder /src/tools      /opt/cassandra/tools
COPY --from=builder /src/pylib      /opt/cassandra/pylib
COPY --from=builder /src/build/apache-cassandra-*.jar /opt/cassandra/build/

# Both SDK jars must also be on the *runtime* classpath. lib/*.jar is globbed by
# cassandra.in.sh:53, so dropping them there is enough.
COPY --from=builder /src/build/lib/jars/antithesis-sdk-${ANTITHESIS_SDK_VERSION}.jar \
                    /src/build/lib/jars/antithesis-ffi-${ANTITHESIS_SDK_VERSION}.jar \
                    /opt/cassandra/lib/

COPY antithesis/docker/node-agent.py           /opt/antithesis/node-agent.py
COPY antithesis/docker/entrypoint-cassandra.sh /opt/antithesis/entrypoint-cassandra.sh
RUN chmod +x /opt/antithesis/entrypoint-cassandra.sh /opt/cassandra/bin/*

# Assertion cataloging AND coverage instrumentation, both enabled by exposing code under
# /opt/antithesis/catalog/. Only the Cassandra jar is cataloged -- instrumenting all 133 dependency
# jars in lib/ would cost build and runtime for no property coverage.
#
# Antithesis follows symlinks exactly one level deep and must not encounter a symlink-to-a-symlink,
# so this is a single symlink to a real directory whose contents are real files.
#
# Coverage instrumentation is also what enables thread-pausing faults, which is the fault type
# that reaches the LocalLog publication CAS window (see
# scratchbook/properties/a-log-processing-never-concurrent.md).
RUN mkdir -p /opt/antithesis/catalog \
 && ln -s /opt/cassandra/build /opt/antithesis/catalog/cassandra

# Run as a non-root user. bin/cassandra refuses to start as root without -R ("Running Cassandra as
# root user or group is not recommended"), and rather than forcing it, use a real user: it matches
# how Cassandra is actually operated, so the harness exercises a more representative configuration.
#
# Everything the process or the control agent writes must be owned by that user:
#   /var/lib/cassandra   data, commitlog, hints, saved_caches, metadata -- and node-agent.py's
#                        wipe-and-restart deletes inside these
#   /opt/cassandra/conf  entrypoint-cassandra.sh rewrites cassandra.yaml in place
#   /opt/cassandra/logs  system.log
#   /etc/cassandra       conf/cassandra-env.sh sets
#                        -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password
#                        unconditionally when LOCAL_JMX=no; created here so the path always exists
RUN groupadd -r cassandra --gid=999 \
 && useradd -r -g cassandra --uid=999 --home-dir=/var/lib/cassandra --shell=/bin/bash cassandra \
 && mkdir -p /var/lib/cassandra/data /var/lib/cassandra/commitlog /var/lib/cassandra/hints \
             /var/lib/cassandra/saved_caches /var/lib/cassandra/accord_journal \
             /opt/cassandra/logs /opt/cassandra/data /etc/cassandra \
 && : > /etc/cassandra/jmxremote.password \
 && chmod 600 /etc/cassandra/jmxremote.password \
 && chown -R cassandra:cassandra /var/lib/cassandra /opt/cassandra/logs /opt/cassandra/conf \
                                 /opt/cassandra/build /opt/cassandra/data /etc/cassandra

USER cassandra

# 7000 internode/gossip+TCM verbs, 7199 JMX, 9042 CQL, 7788 node control agent
EXPOSE 7000 7199 9042 7788

ENTRYPOINT ["/opt/antithesis/entrypoint-cassandra.sh"]
