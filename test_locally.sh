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

echo ">>> Setting up virtualenv for aws sdk"
if [ ! -d "./venv" ]; then
    virtualenv venv
fi
venv/bin/pip install awscli

ant jar

# Extract the actual JAR version from the build directory
CASSANDRA_JAR=$(find build -name "nf-cassandra-*.jar" -type f | head -1 | xargs basename 2>/dev/null || echo "")
if [ -z "$CASSANDRA_JAR" ] || [ ! -f "build/$CASSANDRA_JAR" ]; then
    echo ">>> No Cassandra JAR found in build/ directory"
    exit 1
fi
echo ">>> Using JAR: $CASSANDRA_JAR"
S3URL="s3://netflix-cde-test-genpop/test_priam_$USER/"
newt --app-type awscreds refresh -r persistence_test_cde
venv/bin/aws s3 sync --exclude '*' --include "${CASSANDRA_JAR}" 'build' "${S3URL}"
# reset: sudo rm -Rf /mnt/data/cassandra/data/*/*/s3
yolo2 --regions-parallel --instances-parallel test $APP "
  aws s3 cp --no-progress s3://netflix-cde-test-genpop/test_priam_clohfink/$CASSANDRA_JAR ~
  sudo kill -9 \`cat /run/cassandra/cassandra.pid\` 2>/dev/null || true
  sudo rm /apps/nfcassandra_server/lib/nf-cassandra*.jar
  sudo rm /mnt/data/cassandra/logs/*
  sleep 10
  sudo mv ~/$CASSANDRA_JAR /apps/nfcassandra_server/lib/
  curl -s http://127.0.0.1:8080/Priam/REST/v1/cassadmin/start
"

echo -ne '\007'