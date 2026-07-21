#!/bin/bash
# Cassandra container interactive-shell init -- sourced from /etc/bash.bashrc on
# docker exec sessions. Sets up env/PATH, IPv4/IPv6 address helpers for cqlsh/nodetool,
# convenience aliases and functions, and the status prompt.
# based on https://github.netflix.net/corp/cas-cdecassutils/blob/master/root/etc/profile.d/cass.sh

# --- Environment ---
[ -f /etc/nflx/environment ] && . /etc/nflx/environment
CASS_HOME="${CASS_HOME:-/apps/nfcassandra_server}"
export PATH="${CASS_HOME}/bin:${CASS_HOME}/tools/bin:${PATH}"

# --- Address resolution (IPv4 / IPv6 aware) ---

cass_native_addr() {
    if [ -n "${EC2_LOCAL_IPV4}" ]; then
        printf '%s\n' "${EC2_LOCAL_IPV4}"
        return
    fi
    local addr
    addr=$(hostname -i 2>/dev/null | awk '{print $1; exit}')
    # If hostname resolution fails here, getLocalHost() throws server-side too and the
    # server binds native to the loopback (CASSANDRA-15901) -- so 127.0.0.1 matches.
    printf '%s\n' "${addr:-127.0.0.1}"
}

cass_jmx_host() {
    local h
    for h in 127.0.0.1 ::1; do
        if (exec 3<>"/dev/tcp/${h}/7501") 2>/dev/null; then
            printf '%s\n' "${h}"
            return
        fi
    done
    printf '%s\n' '127.0.0.1'
}

# --- Aliases ---

# --- Nodetool ---
alias nt="${CASS_HOME}/bin/nodetool -h \$(cass_jmx_host) -p 7501"
alias tpstats="nt tpstats | awk '/Pool Name/,/^$/'"

# --- CQL ---
alias cqlsh="${CASS_HOME}/bin/cqlsh \$(cass_native_addr) 7104"
alias cqlshssl="${CASS_HOME}/bin/cqlsh \$(cass_native_addr) 7104 --cqlshrc=${CASS_HOME}/bin/cqlshrc --ssl"

# --- SSTable tools ---
alias ssdump="${CASS_HOME}/tools/bin/sstabledump"
alias ssmeta="${CASS_HOME}/tools/bin/sstablemetadata"

# --- Logs ---
alias tlog='tail -1000f /mnt/data/cassandra/logs/system.log'
alias tlogd='tail -1000f /mnt/data/cassandra/logs/debug.log'
alias vlog='vim /mnt/data/cassandra/logs/system.log'
alias llog='less -iSN /mnt/data/cassandra/logs/system.log'

# --- Navigation ---
alias cl='cd /mnt/data/cassandra/logs'
alias data='cd /mnt/data/cassandra/data'

# --- Process control ---
alias ccstart='rm -f /var/run/nflx-cmd/cass/disabled;echo > /var/run/nflx-cmd/cass/start'
alias ccstop='touch /var/run/nflx-cmd/cass/disabled;echo > /var/run/nflx-cmd/cass/stop'
alias ccstopf='touch /var/run/nflx-cmd/cass/disabled;kill -9 $(cat /run/cassandra/cassandra.pid)'

# --- Diagnostic functions ---
alias backupsnapshotsize='find /mnt/data/cassandra/data/ \( -iname "*backup*" -o -iname "*snapshot*" \) -exec du -h --max-depth=0 {} \;'

show_table() {
    local ks="${1:?Usage: show_table <keyspace> <table>}"
    local tbl="${2:?Usage: show_table <keyspace> <table>}"
    echo "=== Schema ==="
    cqlsh -e "desc \"${ks}\".\"${tbl}\""
    echo "=== Table Stats ==="
    nt tablestats "${ks}.${tbl}"
    echo "=== Histograms ==="
    nt tablehistograms "${ks}" "${tbl}"
}

# --- Prompt ---

# True if a local socket is LISTENing on the given port (hex, as it appears in /proc/net/tcp*), checked across both IPv4 and IPv6.
__tcp_port_listening() {
    local port_hex=${1^^}

    awk -v port="$port_hex" '
        NR > 1 && $4 == "0A" {
            split($2, address, ":")
            if (toupper(address[2]) == port)
                found = 1
        }
        END {
            exit !found
        }
    ' /proc/net/tcp /proc/net/tcp6 2>/dev/null
}

__update_cassandra_prompt() {
    local red='\[\033[1;31m\]'
    local green='\[\033[1;32m\]'
    local dgreen='\[\e[0;32m\]'
    local yellow='\[\033[1;33m\]'
    local cyan='\[\033[1;36m\]'
    local dim='\[\033[2m\]'
    local reset='\[\033[0m\]'

    local check cross
    printf -v check '\u2713'
    printf -v cross '\u2717'

    local cql_status gossip_status timestamp

    timestamp=$(TZ='PST8PDT,M3.2.0/2,M11.1.0/2' date '+%H:%M:%S')

    if __tcp_port_listening 1BC0; then
        cql_status="${green}CQL:${check}${reset}"
    else
        cql_status="${red}CQL:${cross}${reset}"
    fi

    if __tcp_port_listening 1BBF; then
        gossip_status="${green}GOSSIP:${check}${reset}"
    else
        gossip_status="${red}GOSSIP:${cross}${reset}"
    fi

    PS1="\n\
${red}${NETFLIX_ENVIRONMENT:-no-env}${reset} \
${green}${NETFLIX_APP:-no-app}${reset} \
${yellow}${NETFLIX_ZONE:-${EC2_AVAILABILITY_ZONE:-no-zone}}${reset} \
${cyan}${NETFLIX_INSTANCE_ID:-${EC2_INSTANCE_ID:-no-id}}${reset} \
${dgreen}${timestamp} ${reset}${dim}[${reset}${cql_status}${dim}|${reset}${gossip_status}${dim}]${reset}\n\
\w \\$ "
}

# Refresh the prompt before each prompt is drawn (append without clobbering any existing
# PROMPT_COMMAND, and don't double-register if this file is sourced more than once).
case "${PROMPT_COMMAND}" in
    *__update_cassandra_prompt*) ;;
    *) PROMPT_COMMAND="${PROMPT_COMMAND:+${PROMPT_COMMAND}; }__update_cassandra_prompt" ;;
esac
