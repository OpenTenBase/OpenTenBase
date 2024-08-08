FROM ubuntu:22.04

LABEL author="Liu Dongming <ldming101@gmail.com>"

ARG TARGETOS
ARG TARGETARCH
ARG OPENTENBASE_HOME=/var/lib/opentenbase
ARG OPENTENBASE_LIB=/var/lib/opentenbase/lib
ARG OPENTENBASE_BIN=/var/lib/opentenbase/bin
ARG OPENTENBASE_USER=opentenbase
ARG SRC_REPO=https://github.com/OpenTenBase/OpenTenBase
ARG SRC_BRANCH=v2.5.0-release_new

RUN useradd ${OPENTENBASE_USER} -d ${OPENTENBASE_HOME} \
    && mkdir -p ${OPENTENBASE_HOME} \
    && chown -R ${OPENTENBASE_USER}:${OPENTENBASE_USER} ${OPENTENBASE_HOME}

RUN apt update \
    && apt install -y \
        git \
        gcc \
        make \
        libreadline-dev \
        zlib1g-dev \
        libssl-dev \
        libossp-uuid-dev \
        bison \
        flex \
        netcat \
        dnsutils\
        vim \
    && \
    export TMP_OPENTENBASE=$(mktemp -d) \
    && \
    chown -R ${OPENTENBASE_USER}:${OPENTENBASE_USER} ${TMP_OPENTENBASE} \
    && \
    su ${OPENTENBASE_USER} sh -c ' \
        git clone -b ${SRC_BRANCH} ${SRC_REPO} ${TMP_OPENTENBASE} \
        && cd ${TMP_OPENTENBASE} \
        && ./configure --prefix=${OPENTENBASE_HOME} --enable-user-switch --with-openssl --with-ossp-uuid CFLAGS=-g \
        && make clean \
        && make -sj \
        && cd contrib \
        && chmod +x pgxc_ctl/make_signature \
        && make -sj \
    ' \
    && cd ${TMP_OPENTENBASE} \
    && make install \
    && cd contrib \
    && make install \
    && su ${OPENTENBASE_USER} sh -c ' \
        rm -rf ${TMP_OPENTENBASE} \
    ' \
    && apt -y remove \
        git \
        gcc \
        make \
        libreadline-dev \
        zlib1g-dev \
        bison \
        flex \
    && apt -y autoremove \
    && apt -y clean

USER ${OPENTENBASE_USER}

ENV PATH=${OPENTENBASE_BIN}:${PATH} \
    LD_LIBRARY_PATH=${OPENTENBASE_LIB}:${LD_LIBRARY_PATH} \
    LC_ALL=C 

WORKDIR ${OPENTENBASE_HOME}
