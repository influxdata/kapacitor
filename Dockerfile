FROM cr.loongnix.cn/library/debian:buster
LABEL maintainer="yangzewei@loongson.cn" 
ARG RELEASE
ARG LAUNCHPAD_BUILD_ARCH
LABEL org.opencontainers.image.ref.name=debian
LABEL org.opencontainers.image.version=10
CMD ["/bin/bash"]
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg netbase wget tzdata ; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    DEBIAN_FRONTEND=noninteractive apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y bash-completion \
    && awk 'f{if(sub(/^#/,"",$0)==0){f=0}};/^# enable bash completion/{f=1};{print;}' /etc/bash.bashrc > /etc/bash.bashrc.new \
    && mv /etc/bash.bashrc.new /etc/bash.bashrc
ENV KAPACITOR_VERSION=1.6.6
RUN ARCH=$(dpkg --print-architecture); \
    wget https://github.com/yzewei/kapacitor/releases/download/1.6.6/kapacitor_1.6.6.7989708-0_loongarch64.deb; \
    dpkg -i kapacitor_1.6.6.7989708-0_loongarch64.deb; \
    rm kapacitor_1.6.6.7989708-0_loongarch64.deb
COPY  kapacitor.conf /etc/kapacitor/kapacitor.conf
EXPOSE 9092
VOLUME [/var/lib/kapacitor]
#缺entrypoint.sh的内容
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
CMD ["kapacitord"]
