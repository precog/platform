#!/bin/bash

# Parse opts to determine settings
while getopts ":d:lbZYRL:" opt; do
    case $opt in
        d)
            WORKDIR=$(cd $OPTARG; pwd)
            ;;
        l)
            DONTCLEAN=1
            ;;
        L)
            LABCOAT_PORT=$OPTARG
            ;;
        b)
            BUILDMISSING=1
            ;;
        Z)
            TESTQUIT=1
            rm -rf stress-data && mkdir stress-data
            WORKDIR=$(cd stress-data; pwd)
            ;;
        R)
            TESTRESUME=1
            WORKDIR=$(cd stress-data; pwd)
            ;;
        Y)
            ( $0 -Z && $0 -R ) 2>&1 | grep ';;;'
            exit ${PIPESTATUS[0]}
            ;;
        \?)
            echo "Usage: `basename $0` [-blRZ] [-d <work directory>] [-L <port>]"
            echo "  -l: If a temp workdir is used, don't clean up afterward"
            echo "  -L: Use the provided port for labcoat"
            echo "  -d: Use the provided workdir"
            echo "  -b: Build missing artifacts prior to run (depends on sbt in path)"
            echo "  -Y: Run ingest consistency check"
            echo "  -Z: (private to -Y) first pass to be interrupted"
            echo "  -R: (private to -Y) second pass to compelte"
            exit 1
            ;;
    esac
done

[ -n "$TESTQUIT" ] && echo ";;; starting service for test-quit"
[ -n "$TESTRESUME" ] && echo ";;; starting service for test-resume"

# Taken from http://blog.publicobject.com/2006/06/canonical-path-of-file-in-bash.html
function path-canonical-simple() {
    local dst="${1}"
    cd -P -- "$(dirname -- "${dst}")" &> /dev/null && echo "$(pwd -P)/$(basename -- "${dst}")" | sed 's#/\.##'
}

function random_port() {
    # We'll try 100 times until we find an unused port, at which point we give up
    for tryseq in `seq 1 100`; do
        TRYPORT=$((20000 + $RANDOM))
        if ! port_is_open $TRYPORT; then
            echo $TRYPORT
            return 0
        fi
        sleep 1
    done

    echo "Failed to locate unused port for $1!" >&2
    exit 1
}

function port_is_open() {
   netstat -an | egrep "[\.:]$1[[:space:]]+.*LISTEN" > /dev/null
}

function wait_until_port_open () {
    while ! port_is_open $1; do
        sleep 1
    done
}

BASEDIR=$(path-canonical-simple `dirname $0`)/..

echo "Using base: $BASEDIR"

VERSION=`git describe`
DESKTOP_ASSEMBLY="$BASEDIR"/desktop/target/desktop-assembly-$VERSION.jar

GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:-CMSIncrementalPacing -XX:CMSIncrementalDutyCycle=100"

JAVA="java $GC_OPTS"

# pre-flight checks to make sure we have everything we need, and to make sure there aren't any conflicting daemons running
MISSING_ARTIFACTS=""
for ASM in "$DESKTOP_ASSEMBLY"; do
    if [ ! -f "$ASM" ]; then
        if [ -n "$BUILDMISSING" ]; then
            # Darn you, bash! zsh can do this in one go, a la ${$(basename $ASM)%%-*}
            BUILDTARGETBASE=$(basename "$ASM")
            BUILDTARGET="${BUILDTARGETBASE%%-*}/assembly"
            echo "Building $BUILDTARGET"
            pushd .. > /dev/null
            sbt "$BUILDTARGET" || {
                echo "Failed to build $BUILDTARGET!" >&2
                exit 1
            }
            popd > /dev/null
        else
            MISSING_ARTIFACTS="$MISSING_ARTIFACTS $ASM"
        fi
    fi
done


if [ -n "$MISSING_ARTIFACTS" ]; then
    echo "Up-to-date desktop assembly is required before running. Please build and re-run, or run with the -b flag." >&2
    for ASM in $MISSING_ARTIFACTS; do
        echo "  missing `basename $ASM`" >&2
    done
    exit 1
fi

# Make sure we have the tools we need on OSX
if [ `uname` == "Darwin" ]; then
    echo "Running on OSX"
    alias tar="/usr/bin/gnutar"
fi

function exists {
    for fl in "$@"; do
        if [ -f "$fl" ]; then return 0; fi
    done
    return 1
}

## Check for prereqs first
#ARTIFACTDIR="$BASEDIR/standaloneArtifacts"
#
#echo "Using artifacts in $ARTIFACTDIR"
#
#[ -d "$ARTIFACTDIR" ] || mkdir "$ARTIFACTDIR"
#
#(exists "$ARTIFACTDIR"/zookeeper* && echo "  ZooKeeper exists") || {
#    echo "Downloading current ZooKeeper artifact"
#    pushd "$ARTIFACTDIR" > /dev/null
#    wget -nd -q http://ops.reportgrid.com.s3.amazonaws.com/zookeeper/zookeeper-3.4.3.tar.gz || {
#        echo "Failed to download zookeeper" >&2
#        exit 3
#    }
#    popd > /dev/null
#}
#
#(exists "$ARTIFACTDIR"/kafka* && echo "  Kafka exists") || {
#    echo "Downloading current Kafka artifact"
#    pushd "$ARTIFACTDIR" > /dev/null
#    wget -nd -q http://s3.amazonaws.com/ops.reportgrid.com/kafka/kafka-0.7.5.zip || {
#        echo "Failed to download kafka" >&2
#        exit 3
#    }
#    popd > /dev/null
#}

unset REBEL_OPTS
if [ -e "$REBEL_HOME" ]; then
    REBEL_OPTS="-noverify -javaagent:$REBEL_HOME/jrebel.jar -Dplatform.root=`dirname $0`"
else
    REBEL_OPTS=''
fi

if [ "$WORKDIR" == "" ]; then
    WORKDIR=`mktemp -d -t standaloneShard.XXXXXX 2>&1`
    if [ $? -ne 0 ]; then
        echo "Couldn't create temp workdir! ($WORKDIR)" >&2
        exit 1
    fi
else
    # Do *not* allow cleanup of provided directories
    DONTCLEAN=1
fi

# Set up dirs for all components
ZKBASE="$WORKDIR"/zookeeper
ZKDATA="$WORKDIR"/zookeeper-data

KFBASE="$WORKDIR"/kafka
KFDATA="$WORKDIR"/kafka-data

rm -rf $ZKBASE $KFBASE
mkdir -p $ZKBASE $KFBASE $ZKDATA "$WORKDIR"/{configs,logs,shard-data/data,shard-data/archive,shard-data/scratch,shard-data/ingest_failures}

echo "Running standalone shard under $WORKDIR"

# Set shutdown hook
function on_exit() {
    echo "========== Shutting down system =========="

    function is_running() {
        [ ! -z "$1" ] && kill -0 "$1" &> /dev/null
    }


    if is_running $SHARDPID; then
        echo "Stopping shard..."
        kill $SHARDPID
        wait $SHARDPID
    fi

    echo "Shutdown complete"
}

trap on_exit EXIT

ZOOKEEPER_PORT=$(random_port "Zookeeper")
KAFKA_PORT=$(random_port "Kafka global")

# FIXME: There's a potential for collisions here because we're
# assigning before actually starting services, but proper ordering
# would make things a bit more complicated and with bash's RNG this is
# low-risk
SHARD_PORT=$(random_port "Shard")

if [ -z "$LABCOAT_PORT" ]; then
    LABCOAT_PORT=$(random_port "Labcoat")
fi

sed -e "s#/var/log#$WORKDIR/logs#;  s#/opt/precog/shard#$WORKDIR/shard-data#; s/9082/$KAFKA_PORT/; s/2181/$ZOOKEEPER_PORT/; s/port = 30070/port = $SHARD_PORT/; s/port = 30064/port = $ACCOUNTS_PORT/; s/port = 8000/port = $LABCOAT_PORT/; s#/var/kafka#$KFDATA#; s#/var/zookeeper#$ZKDATA#" < "$BASEDIR"/desktop/configs/test/shard-v2.conf > "$WORKDIR"/configs/shard-v2.conf || echo "Failed to update shard config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/desktop/configs/test/shard-v2.logging.xml > "$WORKDIR"/configs/shard-v2.logging.xml

cd "$BASEDIR"

echo "Starting desktop service"
$JAVA -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8123 $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/shard-v2.logging.xml -classpath "$BASEDIR/desktop/precog/precog-desktop.jar" com.precog.shard.desktop.DesktopIngestShardServer --configFile "$WORKDIR"/configs/shard-v2.conf &> $WORKDIR/logs/shard-v2.stdout &
SHARDPID=$!

# Let the ingest/shard services startup in parallel
wait_until_port_open $SHARD_PORT

cat > $WORKDIR/ports.txt <<EOF
KAFKA_PORT=$KAFKA_PORT
ZOOKEEPER_PORT=$ZOOKEEPER_PORT
SHARD_PORT=$SHARD_PORT
LABCOAT_PORT=$LABCOAT_PORT
EOF

echo "Startup complete, running in $WORKDIR"

echo "============================================================"
echo "Base path: $WORKDIR"
cat <<EOF
KAFKA_PORT:        $KAFKA_PORT
ZOOKEEPER_PORT:    $ZOOKEEPER_PORT
SHARD_PORT:        $SHARD_PORT
LABCOAT_PORT:      $LABCOAT_PORT
EOF
echo "============================================================"

function query() {
    curl -s -G \
      --data-urlencode "q=$1" \
      --data-urlencode "apiKey=$ACCOUNTTOKEN" \
      "http://localhost:$SHARD_PORT/analytics/fs/$ACCOUNTID"
}

function count() {
    query "count(//xyz)" | tr -d "[]"
}

function wait_til_nonzero() {
    wait_til_n_rows 1 $1
    return $?
}

function now() {
    date "+%s"
}

function check_time() {
    expr `now` '>' $1
}

function wait_til_n_rows() {
    N=$1
    LIMIT=$( expr `now` '+' $2 )
    RESULT=$( count )
    echo "!!! count returned $RESULT"
    while [ -z "$RESULT" ] || [ "$RESULT" -lt "$N" ]; do
        sleep 0.05
        [ `check_time $LIMIT` -eq 1 ] && return 1
        RESULT=$( count )
        echo "!!! count returned $RESULT"
    done
    return 0
}

function count_lines() {
    wc -l $1 | awk '{print $1}'
}


# Wait forever until the user Ctrl-C's the system
while true; do sleep 30; done
