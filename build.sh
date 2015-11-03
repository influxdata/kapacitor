#!/bin/bash

# Make sure our working dir is the dir of the script
DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $DIR

set -e
# Run the build utility via Docker
docker build --tag=kapacitor-builder $DIR
echo "Running build.py"
docker run -v $DIR:/gopath/src/github.com/influxdb/kapacitor kapacitor-builder "$@"
