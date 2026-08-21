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
# Shared launcher for the workload driver. The helper_ prefix means Antithesis ignores this file
# rather than treating it as a test command.

set -uo pipefail

WORKLOAD_HOME="${WORKLOAD_HOME:-/opt/workload}"

# A classpath directory glob, not `java -jar`. Antithesis injects instrumentation dependency jars
# alongside the application jar, and `-jar` ignores -cp entirely -- so `java -jar` would silently
# run uninstrumented and the workload's assertions would not be cataloged.
exec java \
  -ea \
  -Dfile.encoding=UTF-8 \
  -Xmx256m \
  -Dlogback.configurationFile="${WORKLOAD_HOME}/logback-workload.xml" \
  -cp "${WORKLOAD_HOME}/tcm-workload.jar:${WORKLOAD_HOME}/deps/*" \
  org.apache.cassandra.antithesis.tcm.Main "$@"
