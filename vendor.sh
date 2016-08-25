#!/bin/bash

# Make sure we are in the dir of the script
DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
cd $DIR

match=$1

while read line
do
    spec=($line)
    package=${spec[0]}
    # Skip empty lines and comments
    if [[ -z "$package" || "$package" =~ ^# ]]
    then
        continue
    fi
    uri="https://$package.git"
    dir="vendor/$package"

    if [[ -n "$match" && "$package" != "$match" ]]
    then
        continue
    fi
    echo "Vendoring $package"

    # Determine an upstream branch
    branch=${spec[1]}
    barg=""
    if [ -n "$branch" ]
    then
        barg="-b $branch"
    fi

    if [ -d "$dir" ]
    then
        git subrepo clone --force "$uri" "$dir" $barg
    else
        git subrepo clone "$uri" "$dir" $barg
    fi
done < vendor.list

