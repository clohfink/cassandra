if [ "x$CASSANDRA_HOME" = "x" ]; then
    CASSANDRA_HOME=/apps/nfcassandra_server
fi

. `dirname $0`/cassandra.in.sh.upstream
