#!/bin/bash
# Run the build utility via Docker

set -e

# Make sure our working dir is the dir of the script
DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $DIR

# Unique number for this build
BUILD_NUM=${BUILD_NUM-$RANDOM}
# Home dir of the docker user
HOME_DIR=/root

GO_VERSION=1.15.10

imagename=kapacitor-builder-img-$BUILD_NUM
dataname=kapacitor-data-$BUILD_NUM

# Build new docker image
docker build -f Dockerfile_build_ubuntu64 -t $imagename --build-arg GO_VERSION=${GO_VERSION} $DIR

# Build new docker image
docker build -f Dockerfile_build_ubuntu64 -t influxdata/kapacitor-builder --build-arg GO_VERSION=${GO_VERSION} $DIR

# Create data volume with code
docker create \
    --name $dataname \
    -v "/go/src/github.com/influxdata/kapacitor" \
    $imagename /bin/true

docker cp "$DIR/" "$dataname:/go/src/github.com/influxdata/"

echo "Running build.py"
# Run docker
docker run \
    --rm \
    --volumes-from $dataname \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    $imagename \
    "$@"

docker cp "$dataname:/go/src/github.com/influxdata/kapacitor/build" \
    ./
docker rm -v $dataname
