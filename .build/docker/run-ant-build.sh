#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# A wrapper script to run-tests.sh (or dtest-python.sh) in docker.
#  Can split (or grep) the test list into multiple docker runs, collecting results.

[ $DEBUG ] && set -x

error() {
    echo >&2 $2;
    set -x
    exit $1
}

set -e
# arguments
target=$1
split_chunk="1/1"
[ "$#" -gt 1 ] && split_chunk=$2
java_version=$3
test_script="run-tests.sh"
split_part=${split_chunk%%/*}


echo "Current user: $(whoami)"
echo "Permissions in the current $(pwd) directory:"
ls -ld .

# variables, with defaults
[ "x${cassandra_dir}" != "x" ] || cassandra_dir="$(readlink -f $(dirname "$0")/../..)"

# Make a copy of the cassandra_dir
new_cassandra_dir="${cassandra_dir}_${split_part}"
sudo cp -rp "$cassandra_dir" "$new_cassandra_dir"

# Set cassandra_dir to the new directory
cassandra_dir="$new_cassandra_dir"

[ "x${cassandra_dtest_dir}" != "x" ] || cassandra_dtest_dir="${cassandra_dir}/../cassandra-dtest"
[ "x${build_dir}" != "x" ] || build_dir="${cassandra_dir}/build"
[ "x${m2_dir}" != "x" ] || m2_dir="${HOME}/.m2/repository"
sudo chown -R nfsuper:nac "${HOME}/.m2"
[ -d "${build_dir}" ] || { mkdir -p "${build_dir}" ; }
[ -d "${m2_dir}" ] || { mkdir -p "${m2_dir}" ; }

echo "Running ${target} with split_chunk=${split_chunk} java_version=${java_version} split_part=${split_part} cassandra_dir=${cassandra_dir}"

sudo systemctl status docker

# pre-conditions
command -v docker >/dev/null 2>&1 || { error 1 "docker needs to be installed"; }

docker info >/dev/null 2>&1
if [ $? -ne 0 ]; then
    error 1 "Failed to retrieve Docker info"
fi

(docker info >/dev/null 2>&1) || { error 1 "docker needs to running"; }
[ -f "${cassandra_dir}/build.xml" ] || { error 1 "${cassandra_dir}/build.xml must exist"; }
[ -f "${cassandra_dir}/.build/run-tests.sh" ] || { error 1 "${cassandra_dir}/.build/run-tests.sh must exist"; }

pushd ${cassandra_dir}/.build >/dev/null

# build test image
dockerfile="ubuntu2004_build.docker"
#image_tag="$(md5sum docker/${dockerfile} | cut -d' ' -f1)"
image_tag="0"
image_name="apache/cassandra-${dockerfile/.docker/}:${image_tag}"
docker_mounts="-v ${cassandra_dir}:/home/cassandra/cassandra -v "${build_dir}":/home/cassandra/cassandra/build -v ${m2_dir}:/home/cassandra/.m2/repository"
# HACK hardlinks in overlay are buggy, the following mount prevents hardlinks from being used. ref $TMP_DIR in .build/run-tests.sh
docker_mounts="${docker_mounts} -v "${build_dir}/tmp":/home/cassandra/cassandra/build/tmp"

# Look for existing docker image, otherwise build
if ! ( [[ "$(docker images -q ${image_name} 2>/dev/null)" != "" ]] ) ; then
  echo "Build image not found locally, pulling image ${image_name}..."
  if ! ( docker pull -q ${image_name} >/dev/null 2>/dev/null ) ; then
    # Create build images containing the build tool-chain, Java and an Apache Cassandra git working directory, with retry
    echo "Building docker image..."
    until docker build -t ${image_name} -f docker/${dockerfile} .  ; do
      echo "docker build failed… trying again in 10s… "
      sleep 10
    done
    echo "Docker image ${image_name} has been built"
  else
    echo "Successfully pulled build image."
  fi
else
  echo "Found build image locally."
fi

pushd ${cassandra_dir} >/dev/null

# Optional lookup of Jenkins environment to see how many executors on this machine. `jenkins_executors=1` is used for anything non-jenkins.
jenkins_executors=1
if [[ ! -z ${JENKINS_URL+x} ]] && [[ ! -z ${NODE_NAME+x} ]] ; then
    fetched_jenkins_executors=$(curl -s --retry 9 --retry-connrefused --retry-delay 1 "${JENKINS_URL}/computer/${NODE_NAME}/api/json?pretty=true" | grep 'numExecutors' | awk -F' : ' '{print $2}' | cut -d',' -f1)
    # use it if we got a valid number (despite retry settings the curl above can still fail
    [[ ${fetched_jenkins_executors} =~ '^[0-9]+$' ]] && jenkins_executors=${fetched_jenkins_executors}
fi

echo "Jenkins executors: ${jenkins_executors}"

# find host's available cores and mem
cores=$(docker run --rm alpine:3.19.1 nproc --all) || { error 1 "Unable to check available CPU cores"; }

case $(uname) in
    "Linux")
        mem=$(docker run --rm alpine:3.19.1 free -b | grep Mem: | awk '{print $2}') || { error 1 "Unable to check available memory"; }
        ;;
    "Darwin")
        mem=$(sysctl -n hw.memsize) || { error 1 "Unable to check available memory"; }
        ;;
    *)
        error 1 "Unsupported operating system, expected Linux or Darwin"
esac

# figure out resource limits, scripts, and mounts for the test type
if [[ "${target}" == *"dtest"* ]]; then
    docker_flags="-m 10g --memory-swap 10g"
else
    docker_flags="-m 5g --memory-swap 5g"
fi

docker_flags="${docker_flags} -d --rm"

# make sure build_dir is good
mkdir -p "${build_dir}/tmp" || true
mkdir -p "${build_dir}/test/logs" || true
mkdir -p "${build_dir}/test/output" || true
mkdir -p "${build_dir}/test/reports" || true
chmod -R ag+rwx "${build_dir}"

case "${target}" in
    "cqlsh-test" | "dtest" | "dtest-novnode" | "dtest-latest" | "dtest-large" | "dtest-large-novnode" | "dtest-upgrade" | "dtest-upgrade-large" | "dtest-upgrade-novnode" | "dtest-upgrade-novnode-large" )
        ANT_OPTS="-Dtesttag.extra=_$(arch)_python${python_version/./-}"
    ;;
    "jvm-dtest-novnode" | "jvm-dtest-upgrade-novnode" )
        ANT_OPTS="-Dtesttag.extra=_$(arch)_novnode"
    ;;
    *)
        ANT_OPTS="-Dtesttag.extra=_$(arch)"
    ;;
esac

# the docker container's env
# when we start running dtests we'll need this
#ANT_OPTS="-Dtesttag.extra=_$(arch)_python${python_version/./-}"
#docker_envs="--env JAVA_VERSION=${java_version} --env ANT_OPTS=\"${ANT_OPTS}\""
#docker_envs="--env JAVA_VERSION=${java_version}"
#docker_envs="--env TEST_SCRIPT=${test_script} --env JAVA_VERSION=${java_version}"
docker_envs="--env TEST_SCRIPT=${test_script} --env JAVA_VERSION=${java_version} --env ANT_OPTS=\"${ANT_OPTS}\""

split_str="0_0"
if [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]]; then
    split_str="${split_chunk/\//_}"
fi

echo "Docker env vars: ${docker_envs}"

# git worktrees need their original working directory (in its original path)
if [ -f ${cassandra_dir}/.git ] ; then
    git_location="$(cat ${cassandra_dir}/.git | awk -F".git" '{print $1}' | awk '{print $2}')"
    docker_volume_opt="${docker_volume_opt} -v${git_location}:${git_location}"
fi

random_string="$(LC_ALL=C tr -dc A-Za-z0-9 </dev/urandom | head -c 6 ; echo '')"

container_name="cassandra_${dockerfile/.docker/}_${target}_jdk${java_version/./-}_arch-$(arch)__${split_str}_${random_string}"

logfile="${build_dir}/test/logs/docker_attach_${container_name}.log"

# Docker commands:
#  set java to java_version
#  execute the run_script
#docker_command="source \${CASSANDRA_DIR}/.build/docker/_set_java.sh ${java_version} ; \
#            ant clean build; exit \$?"
unset CASSANDRA_USE_JDK11=true
export CASSANDRA_USE_JDK11=true
#docker_command="source \${CASSANDRA_DIR}/.build/docker/_set_java.sh ${java_version} ; \
#            \${CASSANDRA_DIR}/.build/run-ant-build.sh; exit \$?"
docker_command="source \${CASSANDRA_DIR}/.build/docker/_set_java.sh ${java_version} ; \
            \${CASSANDRA_DIR}/.build/docker/_docker_init_tests.sh ${target} ${split_chunk} ; exit \$?"

METATRON_DIR=/metatron
docker_metatron_flags="--volume "$METATRON_DIR":/metatron --volume /run/metatron:/run/metatron"
# start the container, timeout after 4 hours
docker_id=$(docker run --name ${container_name} ${docker_flags} ${docker_metatron_flags} ${docker_envs} ${docker_mounts} ${docker_volume_opt} ${image_name} sleep 4h)

echo "Running container ${container_name} ${docker_id}"

docker exec --user root ${container_name} bash -c "\${CASSANDRA_DIR}/.build/docker/_create_user.sh cassandra $(id -u) $(id -g)" | tee -a ${logfile}
#docker exec --user root ${container_name} update-alternatives --set python /usr/bin/python${python_version} | tee -a ${logfile}

# capture logs and status
set -o pipefail
docker exec --user cassandra ${container_name} bash -c "${docker_command}" | tee -a ${logfile}
status=$?
set +o pipefail

if [ "$status" -ne 0 ] ; then
    echo "${docker_id} failed (${status}), debug…"
    docker inspect ${docker_id}
    echo "–––"
    docker logs ${docker_id}
    echo "–––"
    docker ps -a
    echo "–––"
    docker info
    echo "–––"
    echo "Failure."
fi
# docker stop in background, ignore errors
( nohup docker stop ${docker_id} >/dev/null 2>/dev/null & )

xz -f ${logfile} 2>/dev/null

popd >/dev/null
popd >/dev/null
set -x
exit ${status}
