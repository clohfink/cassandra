#!/bin/bash

# Source Netflix environment variables (NETFLIX_APP, NETFLIX_REGION, etc.)
[ -f /etc/nflx/environment ] && . /etc/nflx/environment

# ---------------------------------------------------------------------------
# Local vs managed (DGW) mode
#
# In DGW, /etc/nflx/environment provides the NETFLIX_* variables, the token service
# and swappie are reachable over Metatron mTLS, and Cassandra configs are injected at
# INJECTED_CONF by the platform. None of that exists for a plain local docker run, so
# when the env file is absent (or CASSANDRA_LOCAL_MODE=true) we fill in defaults and
# talk to a local plain-HTTP token/swappie server. The managed path is otherwise
# unchanged. See RUNNING_LOCALLY.md.
# ---------------------------------------------------------------------------
if [ ! -f /etc/nflx/environment ] || [ "${CASSANDRA_LOCAL_MODE}" = "true" ]; then
    LOCAL_MODE=true
    : "${NETFLIX_ENVIRONMENT:=local}"
    : "${NETFLIX_REGION:=us-east-1}"
    : "${NETFLIX_APP:=cass_local}"
    : "${NETFLIX_INSTANCE_ID:=local-1}"
    : "${NETFLIX_RUNUSER:=root}"
    : "${NETFLIX_COMMONGROUP:=root}"
    export NETFLIX_ENVIRONMENT NETFLIX_REGION NETFLIX_APP NETFLIX_INSTANCE_ID NETFLIX_RUNUSER NETFLIX_COMMONGROUP
else
    LOCAL_MODE=false
fi

CASS_HOME="${CASS_HOME:-/apps/nfcassandra_server}"
WAKE_FIFO="/var/run/nflx-cmd/cass/start"
STOP_FIFO="/var/run/nflx-cmd/cass/stop"
DISABLE_FILE="/var/run/nflx-cmd/cass/disabled"
TOKEN_MARKER="/var/run/nflx-cmd/cass/has_token"
CONF_DIR="/etc/cassandra"
INJECTED_CONF="/var/lib/nflx-configs/out/cassandra"
LOG_FILE="/logs/cassandra-startup.log"
PID_DIR="/run/cassandra"
PID_FILE="${PID_DIR}/cassandra.pid"
MAX_LOG_FILES=3
MAX_LOG_BYTES=10485760  # 10 MB

# Token service / swappie base URLs. Defaults match the managed DGW endpoints; local runs
# override these (CASSANDRA_TOKEN_SERVICE_URL / CASSANDRA_SWAPPIE_URL) to point at a local
# server. NETFLIX_ENVIRONMENT is set above (env file in DGW, defaults in local mode).
TOKEN_SERVICE_URL="${CASSANDRA_TOKEN_SERVICE_URL:-https://odscasstokens.cluster.us-east-1.${NETFLIX_ENVIRONMENT}.cloud.netflix.net:7004}"
SWAPPIE_URL="${CASSANDRA_SWAPPIE_URL:-https://odscassswappie.cluster.us-east-1.${NETFLIX_ENVIRONMENT}.cloud.netflix.net:7004}"

if [ "${LOCAL_MODE}" = "true" ]; then
    # Point the in-JVM TokenService (seed provider + initial-token lookup) at the same local
    # server. An http:// URL means no Metatron mTLS is used (see TokenService.getConnection).
    export JVM_EXTRA_OPTS="${JVM_EXTRA_OPTS:-} -Dnetflix.tokenservice.url=${TOKEN_SERVICE_URL} -Dnetflix.tokenservice.regions=${NETFLIX_REGION}"

    # Expose JMX off-box so nodetool can reach the node from the host. LOCAL_JMX=no binds JMX on
    # all interfaces; cassandra-env.sh then uses JMX_PORT for both the JMX and RMI ports. Setting
    # rmi.server.hostname=127.0.0.1 makes the RMI stub point at the host loopback, so it works
    # through a 1:1 docker port mapping (JMX_PORT:JMX_PORT). No auth/ssl locally. Set JMX_PORT per
    # node (see RUNNING_LOCALLY.md). These -D flags are appended last, so they override the
    # authenticate=true / hostname the cassandra-env.sh JMX block sets earlier.
    : "${LOCAL_JMX:=no}"
    export LOCAL_JMX
    export JVM_EXTRA_OPTS="${JVM_EXTRA_OPTS} -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=127.0.0.1"
fi

if [ -z "${NETFLIX_RUNUSER}" ] || [ -z "${NETFLIX_COMMONGROUP}" ]; then
    log "ERROR: NETFLIX_RUNUSER or NETFLIX_COMMONGROUP not set"
    exit 1
fi

mkdir -p /var/run/nflx-cmd/cass
chown -R ${NETFLIX_RUNUSER}:${NETFLIX_COMMONGROUP} /var/run/nflx-cmd/cass

mkdir -p "/mnt/data/cassandra"
chown -R "${NETFLIX_RUNUSER}:${NETFLIX_COMMONGROUP}" "/mnt/data/cassandra"

# How to drop privileges to the run user. runuser is present in the managed base image;
# if it is unavailable on a local base, run Cassandra in-process instead.
if command -v runuser >/dev/null 2>&1; then
    RUN_AS=(runuser -u "${NETFLIX_RUNUSER}" --)
else
    RUN_AS=()
fi

# ---------------------------------------------------------------------------
# Rotate startup.log: keep at most MAX_LOG_FILES historical copies.
# startup.log -> startup.log.1 -> … -> startup.log.<MAX_LOG_FILES> (deleted)
# ---------------------------------------------------------------------------
rotate_log() {
    [ ! -f "${LOG_FILE}" ] && return
    local i=${MAX_LOG_FILES}
    while [ $i -gt 1 ]; do
        local prev=$(( i - 1 ))
        [ -f "${LOG_FILE}.${prev}" ] && mv -f "${LOG_FILE}.${prev}" "${LOG_FILE}.${i}"
        i=$prev
    done
    mv -f "${LOG_FILE}" "${LOG_FILE}.1"
}

# Check if this instance has a token assignment.
# Returns 0 (true) if:
#   - token service returns 200 (currently has a token), or
#   - token service has ever returned 400 (had a token before, tracked via marker file)
# Returns 1 (false) if no token has ever been assigned.
has_token() {
    if [ -f "${TOKEN_MARKER}" ]; then
        log "has_token: marker file exists (${TOKEN_MARKER}), returning true"
        return 0
    fi

    local url="${TOKEN_SERVICE_URL}/v1/token/current"
    local response exit_code
    if [ "${LOCAL_MODE}" = "true" ]; then
        response=$(curl -s -f "$url" 2>&1) && exit_code=0 || exit_code=$?
    else
        response=$(metatron curl -a odscasstokens -f "$url" 2>&1) && exit_code=0 || exit_code=$?
    fi

    log "has_token: url=${url}"
    log "has_token: exit_code=${exit_code} response=${response}"

    if [ $exit_code -eq 0 ]; then
        # -f succeeds on 2xx
        log "has_token: success -> has token"
        echo "${response}" > "${TOKEN_MARKER}"
        return 0
    fi

    # Check if the response indicates a 400 (token previously assigned)
    case "${response}" in
        *"400"*|*"Bad Request"*)
            log "has_token: 400 -> touching marker and returning true"
            echo "${response}" > "${TOKEN_MARKER}"
            return 0
            ;;
        *)
            log "has_token: failed -> no token assigned"
            return 1
            ;;
    esac
}

# Poke swappie to check if there's swap work for this instance
poke_swappie() {
    local url="${SWAPPIE_URL}/api/swap/${NETFLIX_ENVIRONMENT}/${NETFLIX_APP}/poke"
    log "poke_swappie: POST ${url}"
    local output exit_code
    if [ "${LOCAL_MODE}" = "true" ]; then
        output=$(curl -s -X POST "$url" 2>&1) && exit_code=0 || exit_code=$?
    else
        output=$(metatron curl -a odscassswappie -X POST "$url" 2>&1) && exit_code=0 || exit_code=$?
    fi
    log "poke_swappie: exit_code=${exit_code} response=${output}"
    if [ $exit_code -ne 0 ]; then
        log "WARNING: failed to poke swappie at ${url}"
    fi
}

# Log to both stdout and the startup log file, rotating if size exceeded
log() {
    if [ -f "${LOG_FILE}" ]; then
        local size
        size=$(stat -c%s "${LOG_FILE}" 2>/dev/null || stat -f%z "${LOG_FILE}" 2>/dev/null || echo "0")
        if [ "${size}" -ge "${MAX_LOG_BYTES}" ]; then
            rotate_log
        fi
    fi
    local ts
    ts=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[${ts}] $*" | tee -a "${LOG_FILE}"
}

# ---------------------------------------------------------------------------
# Merge injected configs over image defaults.
# /etc/cassandra          — image defaults (base layer)
# /var/lib/nflx-configs/… — runtime-injected overrides (take priority)
# ---------------------------------------------------------------------------
merge_configs() {
    [ -d "${INJECTED_CONF}" ] || return
    log "Merging configs from ${INJECTED_CONF} into ${CONF_DIR}"
    for f in "${INJECTED_CONF}"/*; do
        [ -f "$f" ] || continue
        local name
        name=$(basename "$f")
        cp -f "$f" "${CONF_DIR}/${name}"
        log "  merged: ${name}"
    done
}

# ---------------------------------------------------------------------------
# One-time setup
# ---------------------------------------------------------------------------
# Point CASS_HOME/conf at /etc/cassandra — remove any pre-existing dir first
rm -rf "${CASS_HOME}/conf"
ln -sfn "${CONF_DIR}" "${CASS_HOME}/conf"
mkdir -p /usr/share/cassandra
ln -sfn "${CASS_HOME}/bin/cassandra.in.sh" /usr/share/cassandra/cassandra.in.sh
export CASSANDRA_CONF="${CONF_DIR}"

# Require a token assigned by the token service; Cassandra fails fast rather than self-assigning
# random tokens if no assigned token can be obtained. See DatabaseDescriptor.requiresAssignedToken().
export CASSANDRA_REQUIRE_ASSIGNED_TOKEN=true

rotate_log

# ---------------------------------------------------------------------------
# Wait for injected configuration to appear
# ---------------------------------------------------------------------------
while [ ! -f "${INJECTED_CONF}/cassandra.yaml" ]; do
    log "Waiting for configs (${INJECTED_CONF}/cassandra.yaml not found)"
    sleep 5
done
log "Config found: ${INJECTED_CONF}/cassandra.yaml"
merge_configs

# ---------------------------------------------------------------------------
# Retry loop — backoff: 15s, 1m, 5m (then stay at 5m)
# ---------------------------------------------------------------------------
BACKOFFS="15 60 300"
attempt=0

while true; do
    attempt=$(( attempt + 1 ))

    # Check for token assignment — no token means nothing to do yet
    if ! has_token; then
        log "No token assigned, skipping start (attempt ${attempt})"
        poke_swappie
    # Check for disable file — skip launch but keep looping so wake still works
    elif [ -f "${DISABLE_FILE}" ]; then
        log "Cassandra disabled (${DISABLE_FILE} exists), skipping start (attempt ${attempt})"
        poke_swappie
    else
        log "Starting Cassandra (attempt ${attempt}): $@"
        # logs dir needed by jvm gc.log, dont create before this since may not be mounted until token assigned
        if [ ! -d "/mnt/data/cassandra/logs" ]; then
            mkdir -p "/mnt/data/cassandra/logs"
            chown -R "${NETFLIX_RUNUSER}:${NETFLIX_COMMONGROUP}" "/mnt/data/cassandra"
        fi

        # Run Cassandra as NETFLIX_RUNUSER, discard stdout, tee stderr to log file
        "${RUN_AS[@]}" "$@" > /dev/null 2> >(tee -a "${LOG_FILE}" >&2) &

        # store pid for tooling
        CASS_PID=$!
        mkdir -p "${PID_DIR}"
        echo "${CASS_PID}" > "${PID_FILE}"
        log "Cassandra PID: ${CASS_PID}"

        # Set up stop FIFO — writing to it will kill Cassandra: echo > ${STOP_FIFO}
        rm -f "${STOP_FIFO}"
        mkfifo -m 666 "${STOP_FIFO}"
        (
            # Hold the FIFO open read-write so neither our open nor an external
            # writer's open ever blocks, then poll for a stop signal. The watcher
            # also exits on its own as soon as Cassandra is gone, so it never
            # outlives the process it guards — across crash-loop restarts a
            # blocked reader would otherwise leak (and be reparented to PID 1)
            # on every attempt.
            exec 8<>"${STOP_FIFO}"
            while kill -0 "${CASS_PID}" 2>/dev/null; do
                if read -r -t 1 -u 8 _; then
                    log "Stop signal received, killing Cassandra (PID ${CASS_PID})"
                    kill "${CASS_PID}"
                    break
                fi
            done
        ) &
        STOP_WATCHER_PID=$!

        wait "${CASS_PID}"
        exit_code=$?
        # The watcher self-terminates once Cassandra exits (kill -0 fails), so we
        # just reap it — no signal needed, and nothing is left blocked on the FIFO.
        wait "${STOP_WATCHER_PID}" 2>/dev/null
        rm -f "${PID_FILE}" "${STOP_FIFO}"

        log "Cassandra exited with code ${exit_code} (attempt ${attempt})"
    fi

    # Pick the right backoff
    i=1
    delay=300
    for b in ${BACKOFFS}; do
        if [ $i -ge $attempt ]; then
            delay=$b
            break
        fi
        i=$(( i + 1 ))
    done

    # Create a FIFO so external processes can wake us:  echo > /mnt/data/cassandra/start
    rm -f "${WAKE_FIFO}"
    mkfifo -m 666 "${WAKE_FIFO}"
    log "Retrying in ${delay}s ... (echo > ${WAKE_FIFO} to skip)"
    # read will return when either the timeout expires or someone writes to the FIFO
    read -t "$delay" <>"${WAKE_FIFO}" && log "Wake signal received, starting now"
    rm -f "${WAKE_FIFO}"
    merge_configs
done
