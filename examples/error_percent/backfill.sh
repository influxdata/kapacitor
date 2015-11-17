#/bin/bash

set -e

# The load.sh script loads ~11 days of data starting from Jan 01 2015
# This script creates a recording for each day and replays it to the
# Kapacitor task to backfill the 'error_percent' measurement for those
# 11 days
DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
cd $DIR

# Make sure we have defined the task
kapacitor define \
    -name error_percent \
    -type batch \
    -tick error_percent.tick \
    -dbrp pages.default

for day in {1..11}
do
    start=$(printf "2015-01-%02dT00:00:00Z" $day)
    nextday=$(($day + 1))
    stop=$(printf "2015-01-%02dT00:00:00Z" $nextday)

    echo $start $stop
    rid=$(kapacitor record batch -name error_percent -start "$start" -stop "$stop")
    kapacitor replay -name error_percent -id $rid -fast -rec-time
    kapacitor delete recordings $rid
done
