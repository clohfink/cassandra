#!/bin/bash

function usage {
    echo "Usage: test_locally.sh [-s] <app name>"
    echo
    echo "Deploy and test a locally built Cassandra JAR on Netflix infrastructure"
    echo
    echo "Arguments:"
    echo "  app name    Netflix application name to deploy to"
    echo
    echo "Options:"
    echo "  -s          Skip build (use existing JAR in build/)"
    echo
    echo "This script:"
    echo "  1. Builds the Cassandra JAR (unless -s specified)"
    echo "  2. Finds all instances for the specified app"
    echo "  3. Uploads the JAR to instances in batches"
    echo "  4. Restarts Cassandra with the new JAR on all instances"
}

while getopts ":s" opt; do
    case $opt in
        s ) RUN_BUILD=0
        ;;
        \? )
            echo "Invalid option: $OPTARG" >&2
            usage
            exit 1
        ;;
    esac
done

APP=${@:$OPTIND:1}

if [ -z "${1}" ]; then
    usage
    exit 1
fi

set -euf -o pipefail

ant jar

# Extract the actual JAR version from the build directory
CASSANDRA_JAR=$(find build -name "nf-cassandra-*.jar" -type f | head -1 | xargs basename 2>/dev/null || echo "")
if [ -z "$CASSANDRA_JAR" ] || [ ! -f "build/$CASSANDRA_JAR" ]; then
    echo ">>> No Cassandra JAR found in build/ directory"
    exit 1
fi
echo ">>> Using JAR: $CASSANDRA_JAR"

INSTS=$(newt instance-lookup "$APP" | awk 'NR > 2 {print $10}' | grep -E '^i.*')

# Process instances in batches of 3
batch_size=4
inst_array=($INSTS)
total_instances=${#inst_array[@]}

pids=()
for ((i=0; i<total_instances; i+=batch_size)); do
    # Process batch of up to 3 instances
    for ((j=i; j<i+batch_size && j<total_instances; j++)); do
        inst=${inst_array[j]}
        scp "build/$CASSANDRA_JAR" "$inst:~" &
        pids+=($!)
        sleep 5 # need sleep cause if done in parallel too much the %instance magic will not work
    done

    # Wait for current batch to complete before starting next batch
    if ((i+batch_size < total_instances)); then
        echo ">>> Waiting for batch $(((i/batch_size)+1)) to complete..."
        batch_failed=0
        for ((k=${#pids[@]}-batch_size; k<${#pids[@]}; k++)); do
            if ! wait "${pids[k]}"; then
                batch_failed=1
            fi
        done
        if ((batch_failed)); then
            echo ">>> Some uploads in batch $(((i/batch_size)+1)) failed"
        fi
    fi
done

echo ">>> Waiting for all SCP uploads to complete..."
failed=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        failed=1
    fi
done

yolo2 --instances-parallel test $APP "
  sudo kill -9 \`cat /run/cassandra/cassandra.pid\` 2>/dev/null || true
  sudo rm /apps/nfcassandra_server/lib/nf-cassandra*.jar
  sleep 10
  sudo mv ~/$CASSANDRA_JAR /apps/nfcassandra_server/lib/
  curl -s http://127.0.0.1:8080/Priam/REST/v1/cassadmin/start
"