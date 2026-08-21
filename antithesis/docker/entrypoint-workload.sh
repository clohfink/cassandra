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
# Waits for the cluster, emits setup_complete, then idles so Antithesis can run test commands in
# this container.

set -uo pipefail

echo "[workload-entrypoint] waiting for cluster: ${CASSANDRA_NODES}"

# setup_complete must not be emitted until the cluster is genuinely up. Emitting it early means
# faults start landing during cluster formation, and the resulting startup failures look exactly
# like TCM bugs -- which would waste a lot of triage time before anyone realised the harness was at
# fault.
/opt/antithesis/test/v1/tcm/helper_run.sh wait-ready "${WORKLOAD_WAIT_MILLIS:-900000}"
ready=$?

if [[ $ready -ne 0 ]]; then
  echo "[workload-entrypoint] FATAL: cluster did not become ready; not emitting setup_complete."
  echo "[workload-entrypoint] Antithesis will run no test commands, which is the correct outcome:"
  echo "[workload-entrypoint] a run against a cluster that never formed would produce meaningless"
  echo "[workload-entrypoint] property results rather than an obvious failure."
  # Stay alive so the container can be inspected in the debugger rather than exiting and being
  # restarted in a loop.
  exec tail -f /dev/null
fi

# Grow the CMS past its single initial member BEFORE anything else. A freshly initialised cluster has
# a CMS of exactly one node, which is a single point of failure for every metadata commit -- schema
# creation below fails outright if that one member is momentarily unavailable. It also decides whether
# the CMS quorum properties mean anything: with one member every quorum is that member.
echo "[workload-entrypoint] growing the CMS to RF=${WORKLOAD_CMS_RF:-3}"
# Not fatal if it cannot reach the requested RF. The CMS can only draw members from nodes that have
# JOINED the ring, so on a cluster where fewer have joined the request is clamped. A smaller CMS
# degrades what the CMS properties can find -- setup-cms logs exactly which ones and why -- but it
# does not invalidate the other properties, and refusing to run at all would be a worse trade.
if ! /opt/antithesis/test/v1/tcm/helper_run.sh setup-cms "${WORKLOAD_CMS_RF:-3}" "${WORKLOAD_CMS_WAIT_MILLIS:-600000}"; then
  echo "[workload-entrypoint] WARNING: CMS did not reach RF=${WORKLOAD_CMS_RF:-3} (see above)."
  echo "[workload-entrypoint] Continuing: the non-CMS properties are unaffected."
fi

echo "[workload-entrypoint] creating probe schema before signalling readiness"
schema_attempts=0
until /opt/antithesis/test/v1/tcm/helper_run.sh create-schema; do
  schema_attempts=$(( schema_attempts + 1 ))
  if [[ $schema_attempts -ge 10 ]]; then
    echo "[workload-entrypoint] WARNING: probe schema creation failed ${schema_attempts} times;"
    echo "[workload-entrypoint] continuing anyway -- the first_ command retries it under Antithesis."
    break
  fi
  echo "[workload-entrypoint] probe schema attempt ${schema_attempts} failed; retrying in 10s"
  sleep 10
done

/opt/antithesis/setup-complete.sh

echo "[workload-entrypoint] setup_complete emitted; idling for test commands"
exec tail -f /dev/null
