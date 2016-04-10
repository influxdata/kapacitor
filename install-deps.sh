#!/bin/bash

# this script installs the python dependencies required to
# run the kapacitor tests locally.

set -e

INSTALL_PREFIX=${INSTALL_PREFIX:-/usr/local}
PROTO_VERSION=3.0.0-beta-2
# Download and compile protoc
wget https://github.com/google/protobuf/archive/v${PROTO_VERSION}.tar.gz
tar xf v${PROTO_VERSION}.tar.gz
rm -f v${PROTO_VERSION}.tar.gz
pushd protobuf-${PROTO_VERSION}
./autogen.sh
./configure --prefix=${INSTALL_PREFIX}

if ! which nproc >/dev/null 2>&1; then
	nproc() {
		echo 1
	}
fi

make -j $(nproc)
make check
make install
popd

# Install Python Protobuf3
cd protobuf-${PROTO_VERSION}/python
python setup.py install;
