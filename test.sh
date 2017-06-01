#!/bin/bash
#
# This is the Kapacitor test script.
# This script can run tests in different environments.
# # Usage: ./test.sh <environment_index>
# Corresponding environments for environment_index:
#      0: normal 64bit tests
#      1: race enabled 64bit tests
#      2: normal 32bit tests
#      count: print the number of test environments
#      *: to run all tests in parallel containers
#
# Logs from the test runs will be saved in OUTPUT_DIR, which defaults to ./test-logs
#

set -eo pipefail

# Get dir of script and make it is our working directory.
DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
cd $DIR

# Unique number for this build
BUILD_NUM=${BUILD_NUM-$RANDOM}
# Index for which test environment to use
ENVIRONMENT_INDEX=$1
# Set the default OUTPUT_DIR
OUTPUT_DIR=${OUTPUT_DIR-./test-logs}
# Set the default DOCKER_SAVE_DIR
DOCKER_SAVE_DIR=${DOCKER_SAVE_DIR-$HOME/docker}
# Set default parallelism
PARALLELISM=${PARALLELISM-1}
# Set default timeout
TIMEOUT=${TIMEOUT-480s}
# No uncommitted changes
NO_UNCOMMITTED=${NO_UNCOMMITTED-false}
# Home dir of the docker user
HOME_DIR=/root

no_uncomitted_arg="$no_uncommitted_arg"
if [ ! $NO_UNCOMMITTED ]
then
    no_uncomitted_arg=""
fi

# Update this value if you add a new test environment.
ENV_COUNT=3

# Default return code 0
rc=0

# Convert dockerfile name to valid docker image tag name.
function filename2imagename {
    echo ${1/Dockerfile/kapacitor}
}

# Run a test in a docker container
# Usage: run_test_docker <Dockerfile> <env_name>
function run_test_docker {
    local dockerfile=$1
    local imagename=$(filename2imagename "$dockerfile")
    shift
    local name=$1
    shift
    local logfile="$OUTPUT_DIR/${name}.log"

    imagename="$imagename-$BUILD_NUM"
    dataname="kapacitor-data-$BUILD_NUM"

    echo "Building docker image $imagename"
    docker build -f "$dockerfile" -t "$imagename" .

    echo "Running test in docker $name with args $@"

    # Create data volume with code
    docker create \
        --name $dataname \
        -v "$HOME_DIR/go/src/github.com/influxdata/kapacitor" \
        $imagename /bin/true
    docker cp "$DIR/" "$dataname:$HOME_DIR/go/src/github.com/influxdata/"

    # Run tests in docker
    docker run \
         --rm \
         --volumes-from $dataname \
         -e "GORACE=$GORACE" \
         -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
         -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
         "$imagename" \
         "--parallel=$PARALLELISM" \
         "--timeout=$TIMEOUT" \
         "$@" \
         2>&1 | tee "$logfile"

    # Copy results back out
    docker cp \
        "$dataname:$HOME_DIR/go/src/github.com/influxdata/kapacitor/build" \
        ./

    # Remove the data and builder containers
    docker rm -v $dataname
}

if [ ! -d "$OUTPUT_DIR" ]
then
    mkdir -p "$OUTPUT_DIR"
fi

# Run the tests.
case $ENVIRONMENT_INDEX in
    0)
        # 64 bit tests
        run_test_docker Dockerfile_build_ubuntu64 test_64bit --test --generate $no_uncommitted_arg
        rc=$?
        ;;
    1)
        # 64 bit race tests
        GORACE="halt_on_error=1"
        run_test_docker Dockerfile_build_ubuntu64 test_64bit_race --test --generate $no_uncommitted_arg --race
        rc=$?
        ;;
    2)
        # 32 bit tests
        run_test_docker Dockerfile_build_ubuntu32 test_32bit --test --generate $no_uncommitted_arg --arch=i386
        rc=$?
        ;;
    "count")
        echo $ENV_COUNT
        ;;
    *)
        echo "No individual test environment specified running tests for all $ENV_COUNT environments."
        # Run all test environments
        pids=()
        for t in $(seq 0 "$(($ENV_COUNT - 1))")
        do
            $0 $t 2>&1 > /dev/null &
            # add PID to list
            pids+=($!)
        done

        echo "Started all tests. Follow logs in ${OUTPUT_DIR}. Waiting..."

        # Wait for all tests to finish
        for pid in "${pids[@]}"
        do
            wait $pid
            rc=$(($? + $rc))
        done

        # Check if all tests passed
        if [ $rc -eq 0 ]
        then
            echo "All test have passed"
        else
            echo "Some tests failed check logs in $OUTPUT_DIR for results"
        fi
        ;;
esac

exit $rc

