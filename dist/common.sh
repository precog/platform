#!/bin/bash

. config

BASEDIR=$PWD
LIBDIR="$BASEDIR"/lib
ARTIFACTDIR="$BASEDIR"/artifacts

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
