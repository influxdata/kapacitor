FROM 32bit/ubuntu:16.04

# This dockerfile capabable  of the the minumum required
# to run the tests and nothing else.

MAINTAINER support@influxdb.com

RUN apt-get -qq update && apt-get -qq install -y \
    wget \
    unzip \
    git \
    mercurial \
    build-essential \
    autoconf \
    automake \
    libtool \
    python-setuptools \
    zip \
    curl

# Install protobuf3 protoc binary
ENV PROTO_VERSION 3.4.0
RUN wget -q https://github.com/google/protobuf/releases/download/v${PROTO_VERSION}/protoc-${PROTO_VERSION}-linux-x86_32.zip\
    && unzip -j protoc-${PROTO_VERSION}-linux-x86_32.zip bin/protoc -d /bin \
    rm protoc-${PROTO_VERSION}-linux-x86_64.zip

# Install protobuf3 python library
RUN wget -q https://github.com/google/protobuf/releases/download/v${PROTO_VERSION}/protobuf-python-${PROTO_VERSION}.tar.gz \
    && tar -xf protobuf-python-${PROTO_VERSION}.tar.gz \
    && cd /protobuf-${PROTO_VERSION}/python \
    && python setup.py install \
    && rm -rf /protobuf-${PROTO_VERSION} protobuf-python-${PROTO_VERSION}.tar.gz

# Install go
ENV GOPATH /root/go
ENV GO_VERSION 1.11.2
ENV GO_ARCH 386
RUN wget -q https://storage.googleapis.com/golang/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz; \
   tar -C /usr/local/ -xf /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz ; \
   rm /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz
ENV PATH /usr/local/go/bin:$PATH

ENV PROJECT_DIR $GOPATH/src/github.com/influxdata/kapacitor
ENV PATH $GOPATH/bin:$PATH
RUN mkdir -p $PROJECT_DIR
WORKDIR $PROJECT_DIR

VOLUME $PROJECT_DIR

ENTRYPOINT [ "/root/go/src/github.com/influxdata/kapacitor/build.py" ]
