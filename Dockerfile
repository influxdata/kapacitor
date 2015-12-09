# This Dockerfile provides the needed environment to build Kapacitor.
FROM ubuntu:trusty

MAINTAINER support@influxdb.com

# Install deps
RUN apt-get update
RUN apt-get install -y \
    make \
    wget  \
    git \
    mercurial \
    ruby \
    ruby-dev \
    rpm \
    zip \
    python \
    python-boto

RUN gem install fpm

# Install protobuf3
RUN apt-get install -y \
    build-essential \
    autoconf \
    automake \
    libtool \
    python-setuptools \
    curl

ENV PROTO_VERSION 3.0.0-beta-2
# Download and compile protoc
RUN wget https://github.com/google/protobuf/archive/v${PROTO_VERSION}.tar.gz && \
    tar xf v${PROTO_VERSION}.tar.gz && \
    rm -f v${PROTO_VERSION}.tar.gz && \
    cd protobuf-${PROTO_VERSION} && \
    ./autogen.sh && \
    ./configure --prefix=/usr && \
    make -j $(nproc) && \
    make check && \
    make install

# Install Python Protobuf3
RUN cd protobuf-${PROTO_VERSION}/python && \
    python setup.py install;



# Install go
ENV GO_VERSION 1.5.3
ENV GO_ARCH amd64
RUN wget https://storage.googleapis.com/golang/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz; \
   tar -C /usr/local/ -xf /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz ; \
   rm /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz
ENV PATH /usr/local/go/bin:$PATH
ENV GOPATH /gopath
ENV PATH $GOPATH/bin:$PATH
ENV PROJECT_PATH $GOPATH/src/github.com/influxdata/kapacitor
RUN mkdir -p $PROJECT_PATH

WORKDIR $PROJECT_PATH
ENTRYPOINT ["/usr/local/bin/build"]
CMD []

# Get gogo for golang protobuf
RUN go get github.com/gogo/protobuf/protoc-gen-gogo

ADD ./build.py /usr/local/bin/build

