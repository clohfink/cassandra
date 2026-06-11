#!/bin/bash
# Cassandra Docker convenience aliases and functions
# Sourced automatically on interactive bash sessions (docker exec)
# based on https://github.netflix.net/corp/cas-cdecassutils/blob/master/root/etc/profile.d/cass.sh

# --- Environment ---
[ -f /etc/nflx/environment ] && . /etc/nflx/environment
CASS_HOME="${CASS_HOME:-/apps/nfcassandra_server}"
export PATH="${CASS_HOME}/bin:${CASS_HOME}/tools/bin:${PATH}"

# --- Nodetool ---
alias nt="${CASS_HOME}/bin/nodetool -h localhost -p 7501"
alias tpstats="nt tpstats | awk '/Pool Name/,/^$/'"

# --- CQL ---
alias cqlsh="${CASS_HOME}/bin/cqlsh \$(local-ipv4) 7104"
alias cqlshssl="${CASS_HOME}/bin/cqlsh \$(local-ipv4) 7104 --cqlshrc=${CASS_HOME}/bin/cqlshrc --ssl"

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
alias ccstart='rm /var/run/nflx-cmd/cass/disabled;echo > /var/run/nflx-cmd/cass/start'
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
