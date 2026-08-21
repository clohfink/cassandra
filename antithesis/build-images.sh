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
# Builds both harness images. Equivalent to `docker compose build` from antithesis/config, but
# checks the prerequisites first and explains the failures that are otherwise cryptic.

set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/.." && pwd)"

compose="docker compose"
if command -v podman >/dev/null 2>&1 && ! docker info >/dev/null 2>&1; then
  compose="podman compose"
fi

if ! ${compose%% *} info >/dev/null 2>&1; then
  cat <<'EOF' >&2
FATAL: no container runtime is reachable.

Start Docker Desktop (macOS: `open -a Docker`), or install podman, then re-run.
`snouty validate` also needs a running daemon: it inspects the built images to confirm they
target amd64.
EOF
  exit 1
fi

echo "==> building images with: $compose"
echo "    (both Dockerfiles use the repository root as build context; .dockerignore keeps it small)"
cd "$repo/antithesis/config"

# --platform linux/amd64 is set per-service in docker-compose.yaml. Antithesis runs on x86-64, and
# an arm64 image built on Apple silicon will be pulled and then fail to start with an exec format
# error -- after the run has already been submitted.
$compose build "$@"

echo
echo "==> built images:"
for image in cassandra-antithesis-node:latest cassandra-antithesis-workload:latest; do
  arch=$(${compose%% *} image inspect "$image" --format '{{.Architecture}}' 2>/dev/null || echo "?")
  printf '    %-45s arch=%s\n' "$image" "$arch"
  if [[ "$arch" != "amd64" ]]; then
    echo "    WARNING: $image is $arch, not amd64. Antithesis will not be able to run it." >&2
  fi
done

cat <<EOF

Next steps (all local, nothing is submitted):

  snouty validate $repo/antithesis/config
  cd $repo/antithesis/config && $compose up

Once the cluster is up, exercise a single test command by hand:

  $compose exec workload /opt/antithesis/test/v1/tcm/anytime_check_tcm_invariants
  $compose exec workload /opt/antithesis/test/v1/tcm/serial_driver_cms_churn

Assertion output when running outside Antithesis: set ANTITHESIS_SDK_LOCAL_OUTPUT to a file path
in the workload container and the SDK writes the same JSONL it would send to the platform.
EOF
