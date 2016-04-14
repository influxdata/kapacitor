#!/bin/bash

# default options: can be overridden with corresponding arguments.
host=${1-localhost}
port=${2-9100}
games=${3-10}
players=${4-100}

games=$(seq $games)
players=$(seq $players)
# Spam score updates over UDP
while true
do
    for game in $games
    do
        game="g$game"
        for player in $players
        do
            player="p$player"
            score=$(($RANDOM % 1000))
            echo "scores,player=$player,game=$game value=$score" > /dev/udp/$host/$port
        done
    done
    sleep 0.1
done
