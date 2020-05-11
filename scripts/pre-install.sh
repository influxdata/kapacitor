#!/bin/bash

DATA_DIR=/var/lib/kapacitor

# create user
if ! id kapacitor >/dev/null 2>&1; then
    useradd --system -U -M kapacitor -s /bin/false -d $DATA_DIR
fi