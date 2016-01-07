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

# Install go
ENV GO_VERSION 1.5.2
ENV GO_ARCH amd64
RUN wget https://storage.googleapis.com/golang/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz; \
   tar -C /usr/local/ -xf /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz ; \
   rm /go${GO_VERSION}.linux-${GO_ARCH}.tar.gz
ENV PATH /usr/local/go/bin:$PATH
ENV GOPATH /gopath
ENV PROJECT_PATH $GOPATH/src/github.com/influxdata/kapacitor
RUN mkdir -p $PROJECT_PATH

WORKDIR $PROJECT_PATH
ENTRYPOINT ["/usr/local/bin/build"]
CMD []

ADD ./build.py /usr/local/bin/build
