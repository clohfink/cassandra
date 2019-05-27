#!/bin/bash
BASTION="awspersistence.test.netflix.net"
NUM_RUNNING="'sudo jps | grep CassandraDaemon | wc -l'"
RUN_BUILD=1

function usage {
    echo "test_locally.sh <app name>"
    echo
    echo "Executed from the Cassandra directory will build a nf-cassandra-4.0.16.jar, upload it to s3 "
    echo "and live upgrade Cassandra on the destination cluster. Note this only "
    echo "works in test, do not use this on prod! and it only replaces cassandra.jar file, if you have any other deps, "
    echo "this method is not suggested"
    echo
    echo "Options"
    echo "  -s: Don't build and test a fresh jar"
    echo
    echo "Example usage:"
    echo "./test_locally.sh cass_perf_vchella"
    echo "./test_locally.sh -s cass_perf_vchella"
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

echo ">>> Setting up virtualenv for nflx-python-libs and aws sdk"
if [ ! -d "./venv" ]; then
    virtualenv venv
fi
venv/bin/pip install awscli
venv/bin/pip install -i https://smartiproxy.mgmt.netflix.net/pypi nflx-python-libs

# Otherwise you can "test" with a really old deb by accident
if [ $RUN_BUILD == 1 ]; then
    echo ">>> Building fresh Cassandra artifacts, skip with -s"
    echo ">>>"  ant realclean build artifacts
    ant realclean build artifacts
fi

tmp="$(find ./build/dist/lib -name 'nf-cassandra-4*.jar')"
CASSANDRA_JAR=$(basename $tmp)
if [ -z $CASSANDRA_JAR ]; then
    echo ">>> No Cassandra Jar found"
    exit 1
fi
tmp_jar=`echo $CASSANDRA_JAR | cut -f 3 -d "-"`
CASSANDRA_JAR_VERSION=${tmp_jar%.*}
echo "Extracted Cassandra Version: $CASSANDRA_JAR_VERSION"

S3URL="s3://crossaccess.netflix.test/cde/binaries/CDE-NFCASS-PATCH/$CASSANDRA_JAR"

echo ">>> Getting credentials"
echo ">>>" newt --app-type awscreds refresh -r awstest_cde
newt --app-type awscreds refresh -r awstest_cde

echo ">>> Copying local NfCassandra jar to s3"
echo ">>>" venv/bin/aws s3 cp "build/${CASSANDRA_JAR}" "${S3URL}"
venv/bin/aws s3 cp "build/${CASSANDRA_JAR}" "${S3URL}"

echo ">>> Executing bolt from local machine"
echo ">>>" nflx-bolt-run cass_patch_nfcassandra.sh $APP --pack cass --instances-parallel --zones-parallel --regions-parallel --params '&-v='$CASSANDRA_JAR_VERSION'&-r&-f'
NETFLIX_STACK=test NETFLIX_APP=binary_upgrade EC2_REGION=us-west-2 NETFLIX_ENVIRONMENT=test venv/bin/nflx-bolt-run cass_patch_nfcassandra.sh $APP --pack cass --instances-parallel --zones-parallel --regions-parallel --params '&-v='$CASSANDRA_JAR_VERSION'&-r&-f'

echo ">>> Checking if Cassandra started up, you should see 1s next to each machine"
echo ">>>" ssh -t awspersistence.test.netflix.net -- /apps/pae/nflx-python-libs/bin/yolo --all-parallel -z $APP ssh "${NUM_RUNNING}"
ssh -t "$BASTION" -- /apps/pae/nflx-python-libs/bin/yolo --all-parallel -z $APP ssh "${NUM_RUNNING}"
