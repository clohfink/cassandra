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

# Tells Antithesis the system is up and fault injection may begin. Until this is emitted,
# Antithesis runs no test commands and injects no faults.
#
# This must only run once the cluster is genuinely ready — see
# docker/entrypoint-workload.sh, which health-checks all nodes first. Emitting it early means
# faults start landing during cluster formation, which produces startup failures that look like
# TCM bugs.

set -euo pipefail

OUTPUT_PATH="/tmp/antithesis_sdk.jsonl"
if [[ -n "${ANTITHESIS_OUTPUT_DIR:-}" ]]; then
  OUTPUT_PATH="${ANTITHESIS_OUTPUT_DIR}/sdk.jsonl"
  echo "[setup-complete] running in Antithesis, emitting to ${OUTPUT_PATH}"
elif [[ -n "${ANTITHESIS_SDK_LOCAL_OUTPUT:-}" ]]; then
  OUTPUT_PATH="${ANTITHESIS_SDK_LOCAL_OUTPUT}"
  echo "[setup-complete] local SDK output override, emitting to ${OUTPUT_PATH}"
else
  echo "[setup-complete] not in Antithesis, emitting to ${OUTPUT_PATH}"
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
echo '{"antithesis_setup":{"status":"complete","details":{"message":"cassandra TCM cluster ready"}}}' >> "${OUTPUT_PATH}"
