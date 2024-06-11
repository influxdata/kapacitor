#!/bin/bash
# This script is used to build and push a Docker builder image to quay.io.
# You have to set credentials for quay.io in your environment by setting the following variables:
# - QUAY_CD_USER
# - QUAY_CD_PASSWORD

set -x

DOCKER_TAG="kapacitor-$(date +%Y%m%d)"

docker build --rm=false --platform linux/amd64 -f ./Dockerfile_build -t builder:"$DOCKER_TAG" .
docker tag builder:"$DOCKER_TAG" quay.io/influxdb/builder:"$DOCKER_TAG"

docker push quay.io/influxdb/builder:"$DOCKER_TAG"
