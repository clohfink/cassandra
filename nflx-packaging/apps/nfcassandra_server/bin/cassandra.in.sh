if [ "x$CASSANDRA_HOME" = "x" ]; then
    CASSANDRA_HOME=/apps/nfcassandra_server
fi

if [ "x$CASSANDRA_HEAPDUMP_DIR" = "x" ]; then
    CASSANDRA_HEAPDUMP_DIR=/mnt/data/cassandra/dumps
fi

. `dirname $0`/cassandra.in.sh.upstream
