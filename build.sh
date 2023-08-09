#!/bin/bash
# Run the build utility via Docker

set -eux

# Make sure our working dir is the dir of the script
DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $DIR

# Unique number for this build
BUILD_NUM=${BUILD_NUM-$RANDOM}
# Home dir of the docker user
HOME_DIR=/root

imagename=kapacitor-builder-img-$BUILD_NUM

# Build new docker image
docker build -f Dockerfile_build_ubuntu64 -t $imagename $DIR

echo "Running build.py"
# Run docker
docker run \
    --rm \
    -v "$DIR:/kapacitor" \
     $imagename \
     "$@"
    #-e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    #-e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    # $imagename \
    # "$@"
