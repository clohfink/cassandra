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
# Drives the TCM workload against a locally-running cluster and reports whether any property was
# violated. This is NOT a substitute for an Antithesis run: there is no deterministic exploration
# and no injected faults beyond what the workload itself triggers through the node control agent
# (restarts, stops, commit pauses, wipe-and-rejoin). It exercises the plumbing and surfaces any
# assertion that fails on the happy-ish path.
#
# Assertion outcomes are captured via the SDK's local-output mode: every evaluation is appended to
# a JSONL file inside the workload container, which we then analyse. A finding is any
# always / alwaysOrUnreachable / unreachable assertion that ever evaluated false.

set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
cd "$here/config"

ROUNDS="${ROUNDS:-8}"
SDK=/tmp/sdk.jsonl

run() { docker compose exec -T -e ANTITHESIS_SDK_LOCAL_OUTPUT="$SDK" workload \
          /opt/antithesis/test/v1/tcm/helper_run.sh "$@" 2>&1 | grep -E "\[workload\]" | tail -6; }

echo "==> resetting assertion log"
docker compose exec -T workload sh -c ": > $SDK"

echo "==> baseline invariant check"
run check-invariants

for r in $(seq 1 "$ROUNDS"); do
  echo
  echo "==================== round $r/$ROUNDS ===================="
  # A driver command, then the full invariant sweep, mirroring how Antithesis interleaves them.
  case $(( r % 5 )) in
    0) run schema-churn 3 ;;
    1) run membership-churn ;;
    2) run cms-churn ;;
    3) run prepared-check ;;
    4) run coordinator-behind ;;
  esac
  run check-invariants
done

echo
echo "==================== quiet-period recovery ===================="
run check-recovery 300000 300000 180000

echo
echo "==================== ANALYSIS ===================="
docker compose exec -T workload cat "$SDK" > /tmp/tcm-sdk-local.jsonl
python3 "$here/analyze-sdk.py" /tmp/tcm-sdk-local.jsonl
