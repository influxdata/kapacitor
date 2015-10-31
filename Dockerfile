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
    python
RUN gem install fpm

# Install go
RUN wget https://storage.googleapis.com/golang/go1.5.1.linux-amd64.tar.gz; tar -C /usr/local/ -xf /go1.5.1.linux-amd64.tar.gz ; rm /go1.5.1.linux-amd64.tar.gz
ENV PATH $PATH:/usr/local/go/bin
ENV GOPATH /gopath
ENV PROJECT_PATH $GOPATH/src/github.com/influxdb/kapacitor
RUN mkdir -p $PROJECT_PATH

WORKDIR $PROJECT_PATH
ENTRYPOINT ["/usr/local/bin/build"]
CMD []

ADD ./build.py /usr/local/bin/build
