#!/bin/bash

. config

BASEDIR=$PWD
LIBDIR="$BASEDIR"/lib
ARTIFACTDIR="$BASEDIR"/artifacts
DUMPDIR="$BASEDIR"/dump

VERSION=$(cat version)
INGEST_ASSEMBLY="$LIBDIR"/ingest-assembly-$VERSION.jar
AUTH_ASSEMBLY="$LIBDIR"/auth-assembly-$VERSION.jar
ACCOUNTS_ASSEMBLY="$LIBDIR"/accounts-assembly-$VERSION.jar
JOBS_ASSEMBLY="$LIBDIR"/heimdall-assembly-$VERSION.jar
SHARD_ASSEMBLY="$LIBDIR"/shard-assembly-$VERSION.jar
RATATOSKR_ASSEMBLY="$LIBDIR"/ratatoskr-assembly-$VERSION.jar

# Working directories
WORKDIR="$BASEDIR/$WORK"
ZKBASE="$WORKDIR"/zookeeper
ZKDATA="$WORKDIR"/zookeeper-data

KFBASE="$WORKDIR"/kafka
KFGLOBALDATA="$WORKDIR"/kafka-global
KFLOCALDATA="$WORKDIR"/kafka-local

MAX_PORT_OPEN_TRIES=60

function port_is_open() {
   netstat -an | egrep "[\.:]$1[[:space:]]+.*LISTEN" > /dev/null
}

function wait_until_port_open () {
    for tryseq in `seq 1 $MAX_PORT_OPEN_TRIES`; do
        if port_is_open $1; then
            return 0
        fi
        sleep 1
    done
    echo "Time out waiting for open port: $1" >&2
    exit 1
}


