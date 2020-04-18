if [ "x$CASSANDRA_HOME" = "x" ]; then
    CASSANDRA_HOME=/apps/nfcassandra_server
fi

if [ "x$CASSANDRA_HEAPDUMP_DIR" = "x" ]; then
    CASSANDRA_HEAPDUMP_DIR=/mnt/data/cassandra/dumps
fi

# The directory where Cassandra's logs live
# (FIXME, change Priam to use the upstream env variable)
if [ "x$CASS_LOGS_DIR" = "x" ]; then
    export CASS_LOGS_DIR="/mnt/data/cassandra/logs"
fi

if [ "x$CASSANDRA_LOG_DIR" = "x" ] ; then
    export CASSANDRA_LOG_DIR="${CASS_LOGS_DIR}"
fi

. `dirname $0`/cassandra.in.sh.upstream
