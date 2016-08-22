#!/bin/bash
#
# This is the Kapacitor test script.
# This script can run tests in different environments.
# # Usage: ./test.sh <environment_index>
# Corresponding environments for environment_index:
#      0: normal 64bit tests
#      1: race enabled 64bit tests
#      2: normal 32bit tests
#      3: normal 64bit tests against Go 1.6beta2
#      save: build the docker images and save them to DOCKER_SAVE_DIR. Do not run tests.
#      count: print the number of test environments
#      *: to run all tests in parallel containers
#
# Logs from the test runs will be saved in OUTPUT_DIR, which defaults to ./test-logs
#

# Get dir of script and make it is our working directory.
DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
cd $DIR

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

no_uncomitted_arg="$no_uncommitted_arg"
if [ ! $NO_UNCOMMITTED ]
then
    no_uncomitted_arg=""
fi


# Default to deleteing the container
DOCKER_RM=${DOCKER_RM-true}

# Update this value if you add a new test environment.
ENV_COUNT=4

# Default return code 0
rc=0

# Executes the given statement, and exits if the command returns a non-zero code.
function exit_if_fail {
    command=$@
    echo "Executing '$command'"
    $command
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "'$command' returned $rc."
        exit $rc
    fi
}

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

    build_docker_image "$dockerfile" "$imagename"
    echo "Running test in docker $name with args $@"

    docker run \
         --rm=$DOCKER_RM \
         -v "$DIR:/root/go/src/github.com/influxdata/kapacitor" \
         -e "INFLUXDB_DATA_ENGINE=$INFLUXDB_DATA_ENGINE" \
         -e "GORACE=$GORACE" \
         -e "GO_CHECKOUT=$GO_CHECKOUT" \
	 -e "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID" \
	 -e "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
         "$imagename" \
         "--parallel=$PARALLELISM" \
         "--timeout=$TIMEOUT" \
         "$@" \
         2>&1 | tee "$logfile"
    return "${PIPESTATUS[0]}"

}

# Build the docker image defined by given dockerfile.
function build_docker_image {
    local dockerfile=$1
    local imagename=$2

    echo "Building docker image $imagename"
    exit_if_fail docker build -f "$dockerfile" -t "$imagename" .
}


# Saves a docker image to $DOCKER_SAVE_DIR
function save_docker_image {
    local dockerfile=$1
    local imagename=$(filename2imagename "$dockerfile")
    local imagefile="$DOCKER_SAVE_DIR/${imagename}.tar.gz"

    if [ ! -d  "$DOCKER_SAVE_DIR" ]
    then
        mkdir -p "$DOCKER_SAVE_DIR"
    fi

    if [[ -e "$imagefile" ]]
    then
        zcat $imagefile | docker load
    fi
    imageid=$(docker images -q --no-trunc "$imagename")
    build_docker_image "$dockerfile" "$imagename"
    newimageid=$(docker images -q --no-trunc "$imagename")
    rc=0
    if [ "$imageid" != "$newimageid" ]
    then
        docker save "$imagename" | gzip > "$imagefile"
        rc="${PIPESTATUS[0]}"
    fi
    return "$rc"
}

if [ ! -d "$OUTPUT_DIR" ]
then
    mkdir -p "$OUTPUT_DIR"
fi

# Run the tests.
case $ENVIRONMENT_INDEX in
    0)
        # 64 bit tests
        run_test_docker Dockerfile_build_ubuntu64 test_64bit --debug --test --generate $no_uncommitted_arg
        rc=$?
        ;;
    1)
        # 64 bit race tests
        GORACE="halt_on_error=1"
        run_test_docker Dockerfile_build_ubuntu64 test_64bit_race --debug --test --generate $no_uncommitted_arg --race
        rc=$?
        ;;
    2)
        # 32 bit tests
        run_test_docker Dockerfile_build_ubuntu32 test_32bit --debug --test --generate $no_uncommitted_arg --arch=i386
        rc=$?
        ;;
    3)
        # 64 bit tests on golang go1.6
        GO_CHECKOUT=go1.6rc2
        run_test_docker Dockerfile_build_ubuntu64_git test_64bit_go1.6 --debug --test --generate $no_uncommitted_arg
        rc=$?
        ;;
    "save")
        # Save docker images for every Dockerfile_build* file.
        # Useful for creating an external cache.
        pids=()
        tail_cmds=()
        for d in Dockerfile_build*
        do
            echo "Building and saving $d ..."
            out="$OUTPUT_DIR/${d}.log"
            save_docker_image "$d" > "$out" 2>&1 &
            pid=$!
            pids+=($pid)
            tail_cmds+=("tail -n +0 --pid $pid -F $out")
        done
        echo "Waiting..."
        # Wait for all saves to finish
        i=0
        for pid in "${pids[@]}"
        do
            tail_cmd="${tail_cmds[$i]}"
            $tail_cmd
            wait $pid
            rc=$(($? + $rc))
            i=$((i+1))
        done
        # Check if all saves passed
        if [ $rc -eq 0 ]
        then
            echo "All saves succeeded"
        else
            echo "Some saves failed, check logs in $OUTPUT_DIR"
        fi
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

