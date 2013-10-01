## 
##  ____    ____    _____    ____    ___     ____ 
## |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
## | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
## |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
## |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
## 
## This program is free software: you can redistribute it and/or modify it under the terms of the 
## GNU Affero General Public License as published by the Free Software Foundation, either version 
## 3 of the License, or (at your option) any later version.
## 
## This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
## without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
## the GNU Affero General Public License for more details.
## 
## You should have received a copy of the GNU Affero General Public License along with this 
## program. If not, see <http://www.gnu.org/licenses/>.
## 
## 
#!/bin/bash

# Taken from http://blog.publicobject.com/2006/06/canonical-path-of-file-in-bash.html
function path-canonical-simple() {
    local dst="${1}"
    cd -P -- "$(dirname -- "${dst}")" &> /dev/null && echo "$(pwd -P)/$(basename -- "${dst}")" | sed 's#/\.##'
}

BASEDIR=$(path-canonical-simple `dirname $0`)/..

echo "Using base: $BASEDIR"

CONFIGFILE="$BASEDIR"/desktop/configs/test/bifrost-v2.conf
DESCRIPTION="service"

# Parse opts to determine settings
while getopts ":ad:lbZYRL:" opt; do
    case $opt in
        a)
            echo "Using app (GUI) config"
            DESCRIPTION="app"
            CONFIGFILE="$BASEDIR"/desktop/configs/test/bifrost-v2-app.conf
            ;;
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
            echo "Usage: `basename $0` [-ablRZ] [-d <work directory>] [-L <port>]"
            echo "  -a: Use the app (GUI) config"
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


VERSION=`git describe`
DESKTOP_ASSEMBLY="$BASEDIR"/desktop/target/desktop-assembly-$VERSION.jar

GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:-CMSIncrementalPacing -XX:CMSIncrementalDutyCycle=100"

JAVA="java $GC_OPTS"

function exists {
    for fl in "$@"; do
        if [ -f "$fl" ]; then return 0; fi
    done
    return 1
}

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
mkdir -p $ZKBASE $KFBASE $ZKDATA "$WORKDIR"/{configs,logs,bifrost-data/data,bifrost-data/archive,bifrost-data/scratch,bifrost-data/ingest_failures}

echo "Running standalone bifrost under $WORKDIR"

function is_running() {
    [ ! -z "$1" ] && kill -0 "$1" &> /dev/null
}

# Set shutdown hook
function on_exit() {
    echo "========== Shutting down system =========="

    if is_running $SHARDPID; then
        echo "Stopping bifrost..."
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

sed -e "s#/var/log#$WORKDIR/logs#;  s#/opt/precog/bifrost#$WORKDIR/bifrost-data#; s/9082/$KAFKA_PORT/; s/2181/$ZOOKEEPER_PORT/; s/port = 30070/port = $SHARD_PORT/; s/port = 8000/port = $LABCOAT_PORT/; s#/var/kafka#$KFDATA#; s#/var/zookeeper#$ZKDATA#" < $CONFIGFILE > "$WORKDIR"/configs/bifrost-v2.conf || echo "Failed to update bifrost config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/desktop/configs/test/bifrost-v2.logging.xml > "$WORKDIR"/configs/bifrost-v2.logging.xml

cd "$BASEDIR"

echo "Starting desktop $DESCRIPTION"

if [ "$DESCRIPTION" = "app" ]; then
    cd $WORKDIR # Since the app version is .-relative for config
fi

$JAVA -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8123 $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/bifrost-v2.logging.xml -classpath "$BASEDIR/desktop/precog/precog-desktop.jar" com.precog.bifrost.desktop.LaunchLabcoat --configFile "$WORKDIR"/configs/bifrost-v2.conf &> $WORKDIR/logs/bifrost-v2.stdout &
SHARDPID=$!

# Let the ingest/bifrost services startup in parallel
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

# Wait forever until the user exits the system
while is_running $SHARDPID; do sleep 1; done
