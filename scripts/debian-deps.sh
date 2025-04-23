#!/usr/bin/env bash

set -x
# APT packages
apt-get -qq update && apt-get -qq install -y \
    software-properties-common \
    unzip \
    mercurial \
    make \
    ruby \
    ruby-dev \
    rpm \
    zip \
    python3 \
    python3-setuptools \
    python3-boto \
    python3-pip \
    autoconf \
    automake \
    libtool