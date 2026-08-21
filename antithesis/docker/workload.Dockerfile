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
# Workload driver image for the Antithesis TCM harness.
#
# Build context is the repository root:
#   docker build --platform linux/amd64 -f antithesis/docker/workload.Dockerfile -t tcm-workload:latest .

# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------
FROM docker.io/library/eclipse-temurin:17-jdk-jammy AS builder

ARG ANTITHESIS_SDK_VERSION=1.6.0
ARG DEBIAN_FRONTEND=noninteractive
ENV NO_COLOR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        ant curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY antithesis/workload /build
COPY lib /cassandra-lib

# Assemble the dependency set. Only five artifacts are needed, and three of them already ship with
# Cassandra -- so the workload adds no new dependency beyond the Antithesis SDK itself:
#
#   cassandra-driver-core *shaded*  CQL. The shaded build relocates netty but NOT guava: its public
#                                   API still references com.google.common.base.Predicate (in
#                                   WhiteListPolicy's constructor), so guava is needed to compile
#                                   against it, and guava's failureaccess companion is needed at
#                                   runtime. Both were found by compiling, not by reading docs.
#   guava + failureaccess           see above.
#   jackson-databind/core/annotations  required by the SDK: every assertion's `details` parameter is
#                                   a com.fasterxml.jackson.databind.node.ObjectNode.
#   slf4j-api + logback             the driver logs through slf4j; without a binding it warns on
#                                   every call and the output is noise in triage.
#
# lib/ carries several versions of some of these (slf4j-api 1.7.25 and 2.0.17, logback 1.2.9 and
# 1.5.18, guava 27 and 32). Picking the highest by version sort rather than hardcoding means a
# dependency bump in lib/ does not silently break this image; a missing match is a hard failure
# rather than a silently short classpath.
RUN set -eu; \
    mkdir -p /build/deps; \
    pick() { \
      match=$(ls /cassandra-lib/$1 2>/dev/null | sort -V | tail -1); \
      if [ -z "$match" ]; then echo "FATAL: no jar in lib/ matching $1" >&2; exit 1; fi; \
      echo "  using $(basename "$match")"; \
      cp "$match" /build/deps/; \
    }; \
    pick 'cassandra-driver-core-*-shaded.jar'; \
    pick 'guava-*.jar'; \
    pick 'failureaccess-*.jar'; \
    pick 'jackson-databind-*.jar'; \
    pick 'jackson-core-*.jar'; \
    pick 'jackson-annotations-*.jar'; \
    pick 'slf4j-api-*.jar'; \
    pick 'logback-classic-*.jar'; \
    pick 'logback-core-*.jar'

# The SDK needs both jars: com.antithesis:sdk depends on com.antithesis:ffi.
RUN for artifact in sdk ffi; do \
      curl --fail --location --silent --show-error \
        -o /build/deps/antithesis-${artifact}-${ANTITHESIS_SDK_VERSION}.jar \
        https://repo1.maven.org/maven2/com/antithesis/${artifact}/${ANTITHESIS_SDK_VERSION}/${artifact}-${ANTITHESIS_SDK_VERSION}.jar; \
    done \
 && ls -l /build/deps

RUN ant jar

# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------
FROM docker.io/library/eclipse-temurin:17-jdk-jammy

ARG DEBIAN_FRONTEND=noninteractive

ENV NO_COLOR=1 \
    FORCE_COLOR=0 \
    WORKLOAD_HOME=/opt/workload \
    WORKLOAD_STATE_DIR=/var/lib/antithesis-workload \
    CASSANDRA_NODES=cassandra-1,cassandra-2,cassandra-3,cassandra-4,cassandra-5

# curl and netcat for the entrypoint health check and for poking at the node control agents by hand
# inside the multiverse debugger, where there is no package manager.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl netcat-openbsd procps iproute2 iputils-ping python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/build/tcm-workload.jar  /opt/workload/tcm-workload.jar
COPY --from=builder /build/deps                    /opt/workload/deps
COPY --from=builder /build/logback-workload.xml    /opt/workload/logback-workload.xml

COPY antithesis/test/v1/tcm                  /opt/antithesis/test/v1/tcm
COPY antithesis/setup-complete.sh            /opt/antithesis/setup-complete.sh
COPY antithesis/docker/entrypoint-workload.sh /opt/antithesis/entrypoint-workload.sh

RUN chmod +x /opt/antithesis/setup-complete.sh \
             /opt/antithesis/entrypoint-workload.sh \
             /opt/antithesis/test/v1/tcm/*

# Assertion cataloging is required for SDK assertions to be recognised, and the workload's
# assertions live in this jar -- so the workload jar must be cataloged just as the Cassandra jar is
# in the node image. One symlink to a real file, since Antithesis follows only one symlink level.
RUN mkdir -p /opt/antithesis/catalog \
 && ln -s /opt/workload/tcm-workload.jar /opt/antithesis/catalog/tcm-workload.jar

RUN mkdir -p ${WORKLOAD_STATE_DIR}

ENTRYPOINT ["/opt/antithesis/entrypoint-workload.sh"]
