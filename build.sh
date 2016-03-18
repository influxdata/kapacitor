#!/bin/bash
# Run the build utility via Docker

set -e

# Make sure our working dir is the dir of the script
DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $DIR

# Build new docker image
docker build -f Dockerfile_build_ubuntu64 -t influxdata/kapacitor-builder $DIR
if test "${KAPACITOR_USE_BUILD_CACHE}" = "true"; then
	# create a container that owns the /root/go/src volume used as a cache of go dependency downloads
	# ignore failures, since this usually means the container already exists.
	docker run --entrypoint=/bin/true --name kapacitor-builder-cache influxdata/kapacitor-builder 2>/dev/null || true
	VOLUME_OPTIONS="--volumes-from kapacitor-builder-cache"
else
	VOLUME_OPTIONS=
fi

echo "Running build.py"
# Run docker
docker run --rm \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    ${VOLUME_OPTIONS} \
    -v $DIR:/root/go/src/github.com/influxdata/kapacitor \
    influxdata/kapacitor-builder \
    "$@"
