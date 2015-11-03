#!/bin/bash

# Make sure our working dir is the dir of the script
DIR=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
cd $DIR

set -e
# Run the build utility via Docker
docker build --tag=kapacitor-builder $DIR
echo "Running build.py"
docker run -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" -v $DIR:/gopath/src/github.com/influxdb/kapacitor kapacitor-builder "$@"
