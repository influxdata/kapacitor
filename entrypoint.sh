#!/bin/bash
set -eux

if [ "${1:0:1}" = '-' ]; then
    set -- kapacitord "$@"
fi

KAPACITOR_HOSTNAME=${KAPACITOR_HOSTNAME:-$HOSTNAME}
export KAPACITOR_HOSTNAME

exec "$@"

