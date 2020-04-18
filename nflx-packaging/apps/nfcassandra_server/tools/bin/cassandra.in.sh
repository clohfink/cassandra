if [ "x$CASSANDRA_HOME" = "x" ]; then
    CASSANDRA_HOME=/apps/nfcassandra_server
fi

if [ "x$CASSANDRA_LOG_DIR" = "x" ] ; then
    export CASSANDRA_LOG_DIR="/mnt/data/cassandra/logs"
fi

. `dirname $0`/cassandra.in.sh.upstream
