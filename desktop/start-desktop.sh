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

# Parse opts to determine settings
while getopts ":d:lbZYR" opt; do
    case $opt in
        d)
            WORKDIR=$(cd $OPTARG; pwd)
            ;;
        l)
            DONTCLEAN=1
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
            echo "Usage: `basename $0` [-l] [-d <work directory>]"
            echo "  -l: If a temp workdir is used, don't clean up afterward"
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
YGGDRASIL_ASSEMBLY="$BASEDIR"/yggdrasil/target/yggdrasil-assembly-$VERSION.jar

GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:-CMSIncrementalPacing -XX:CMSIncrementalDutyCycle=100"

JAVA="java $GC_OPTS"

# pre-flight checks to make sure we have everything we need, and to make sure there aren't any conflicting daemons running
MISSING_ARTIFACTS=""
for ASM in "$DESKTOP_ASSEMBLY" "$YGGDRASIL_ASSEMBLY"; do
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
    echo "Up-to-date desktop and yggdrasil assemblies are required before running. Please build and re-run, or run with the -b flag." >&2
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

# Check for prereqs first
ARTIFACTDIR="$BASEDIR/standaloneArtifacts"

echo "Using artifacts in $ARTIFACTDIR"

[ -d "$ARTIFACTDIR" ] || mkdir "$ARTIFACTDIR"

(exists "$ARTIFACTDIR"/zookeeper* && echo "  ZooKeeper exists") || {
    echo "Downloading current ZooKeeper artifact"
    pushd "$ARTIFACTDIR" > /dev/null
    wget -nd -q http://ops.reportgrid.com.s3.amazonaws.com/zookeeper/zookeeper-3.4.3.tar.gz || {
        echo "Failed to download zookeeper" >&2
        exit 3
    }
    popd > /dev/null
}

(exists "$ARTIFACTDIR"/kafka* && echo "  Kafka exists") || {
    echo "Downloading current Kafka artifact"
    pushd "$ARTIFACTDIR" > /dev/null
    wget -nd -q http://s3.amazonaws.com/ops.reportgrid.com/kafka/kafka-0.7.5.zip || {
        echo "Failed to download kafka" >&2
        exit 3
    }
    popd > /dev/null
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
KFGLOBALDATA="$WORKDIR"/kafka-global
KFLOCALDATA="$WORKDIR"/kafka-local

rm -rf $ZKBASE $KFBASE
mkdir -p $ZKBASE $KFBASE $ZKDATA "$WORKDIR"/{configs,logs,shard-data/data,shard-data/archive,shard-data/scratch,shard-data/ingest_failures}

echo "Running standalone shard under $WORKDIR"

# Set shutdown hook
function on_exit() {
    echo "========== Shutting down system =========="

    function is_running() {
        [ ! -z "$1" ] && kill -0 "$1" &> /dev/null
    }

    if is_running $INGESTPID; then
        echo "Stopping ingest..."
        kill $INGESTPID
        wait $INGESTPID
    fi

    if is_running $SHARDPID; then
        echo "Stopping shard..."
        kill $SHARDPID
        wait $SHARDPID
    fi

    if is_running $KFGLOBALPID; then
        echo "Stopping kafka..."
        # Kafka is somewhat of a pain, since the Java process daemonizes from within the startup script. That means that killing the script detaches
        # the Java process, leaving it running. Instead, we kill all child processes
        for pid in `ps -o pid,ppid | awk -v PID=$KFGLOBALPID '{ if($2 == PID) print $1}'`; do kill $pid; done
        wait $KFGLOBALPID
    fi

    if is_running $KFLOCALPID; then
        for pid in `ps -o pid,ppid | awk -v PID=$KFLOCALPID '{ if($2 == PID) print $1}'`; do kill $pid; done
        wait $KFLOCALPID
    fi

    echo "Stopping zookeeper..."
    cd $ZKBASE/bin
    ./zkServer.sh stop

    if [ -z "$DONTCLEAN" ]; then
        echo "Cleaning up temp work dir"
        rm -rf "$WORKDIR"
    fi

    echo "Shutdown complete"
}

trap on_exit EXIT

# Get zookeeper up and running first
pushd $ZKBASE > /dev/null
tar --strip-components=1 --exclude='docs*' --exclude='src*' --exclude='dist-maven*' --exclude='contrib*' --exclude='recipes*' -xvzf "$ARTIFACTDIR"/zookeeper* > /dev/null 2>&1 || {
    echo "Failed to unpack zookeeper" >&2
    exit 3
}
popd > /dev/null

# Copy in a simple config
ZOOKEEPER_PORT=$(random_port "Zookeeper")
echo "# the directory where the snapshot is stored." >> $ZKBASE/conf/zoo.cfg
echo "dataDir=$ZKDATA" >> $ZKBASE/conf/zoo.cfg
echo "# the port at which the clients will connect" >> $ZKBASE/conf/zoo.cfg
echo "clientPort=$ZOOKEEPER_PORT" >> $ZKBASE/conf/zoo.cfg

# Set up logging for zookeeper
cat > $ZKBASE/bin/log4j.properties <<EOF
log4j.rootLogger=INFO, file
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=$WORKDIR/logs/zookeeper.log
log4j.appender.file.MaxFileSize=1MB
log4j.appender.file.MaxBackupIndex=1
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{ABSOLUTE} %5p %c{1}:%L - %m%n
EOF

# Start it up!
echo "Starting zookeeper on port $ZOOKEEPER_PORT"
cd $ZKBASE/bin
./zkServer.sh start &> $WORKDIR/logs/zookeeper.stdout

wait_until_port_open $ZOOKEEPER_PORT

# Now, start global and local kafkas
cd "$WORKDIR"
unzip "$ARTIFACTDIR"/kafka* > /dev/null || {
    echo "Failed to unpack kafka" >&2
    exit 3
}

# Transform the provided config into global and local configs, and start services
cd "$WORKDIR"/kafka/config
chmod +x $KFBASE/bin/kafka-server-start.sh

KAFKA_GLOBAL_PORT=$(random_port "Kafka global")
sed -e "s#log.dir=.*#log.dir=$KFGLOBALDATA#; s/port=.*/port=$KAFKA_GLOBAL_PORT/; s/zk.connect=localhost:2181/zk.connect=localhost:$ZOOKEEPER_PORT/" < server.properties > server-global.properties
$KFBASE/bin/kafka-server-start.sh $KFBASE/config/server-global.properties &> $WORKDIR/logs/kafka-global.stdout &
KFGLOBALPID=$!

wait_until_port_open $KAFKA_GLOBAL_PORT

KAFKA_LOCAL_PORT=$(random_port "Kafka local")
sed -e "s#log.dir=.*#log.dir=$KFLOCALDATA#; s/port=.*/port=$KAFKA_LOCAL_PORT/; s/enable.zookeeper=.*/enable.zookeeper=false/; s/zk.connect=localhost:2181/zk.connect=localhost:$ZOOKEEPER_PORT/" < server.properties > server-local.properties
$KFBASE/bin/kafka-server-start.sh $KFBASE/config/server-local.properties &> $WORKDIR/logs/kafka-local.stdout &
KFLOCALPID=$!

wait_until_port_open $KAFKA_LOCAL_PORT

echo "Kafka Global = $KFGLOBALPID"
echo "Kafka Local = $KFLOCALPID"

# FIXME: There's a potential for collisions here because we're
# assigning before actually starting services, but proper ordering
# would make things a bit more complicated and with bash's RNG this is
# low-risk
INGEST_PORT=$(random_port "Ingest")
SHARD_PORT=$(random_port "Shard")

# Set up ingest and shard service configs
sed -e "s#/var/log#$WORKDIR/logs#; s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#; s#/accounts/v1/#/#; s/port = 9082/port = $KAFKA_LOCAL_PORT/; s/port = 9092/port = $KAFKA_GLOBAL_PORT/; s/connect = localhost:2181/connect = localhost:$ZOOKEEPER_PORT/; s/port = 30060/port = $INGEST_PORT/" < "$BASEDIR"/desktop/configs/test/ingest-v1.conf > "$WORKDIR"/configs/ingest-v1.conf || echo "Failed to update ingest config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/desktop/configs/test/ingest-v1.logging.xml > "$WORKDIR"/configs/ingest-v1.logging.xml

sed -e "s#/var/log#$WORKDIR/logs#; s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#; s#/opt/precog/shard#$WORKDIR/shard-data#; s/port = 9092/port = $KAFKA_GLOBAL_PORT/; s/hosts = localhost:2181/hosts = localhost:$ZOOKEEPER_PORT/; s/port = 30070/port = $SHARD_PORT/; s/port = 30064/port = $ACCOUNTS_PORT/" < "$BASEDIR"/desktop/configs/test/shard-v1.conf > "$WORKDIR"/configs/shard-v1.conf || echo "Failed to update shard config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/desktop/configs/test/shard-v1.logging.xml > "$WORKDIR"/configs/shard-v1.logging.xml

cd "$BASEDIR"

# Prior to ingest startup, we need to set an initial checkpoint if it's not already there
if [ ! -e "$WORKDIR"/initial_checkpoint.json ]; then
    $JAVA $REBEL_OPTS -jar "$YGGDRASIL_ASSEMBLY" zk -z "localhost:$ZOOKEEPER_PORT" -uc "/precog-desktop/shard/checkpoint/`hostname`:{\"offset\":0, \"messageClock\":[]}" &> $WORKDIR/logs/checkpoint_init.stdout || {
        echo "Couldn't set initial checkpoint!" >&2
        exit 3
    }
    touch "$WORKDIR"/initial_checkpoint.json
fi

echo "Starting ingest service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/ingest-v1.logging.xml -classpath "$DESKTOP_ASSEMBLY" com.precog.standalone.StandaloneIngestServer --configFile "$WORKDIR"/configs/ingest-v1.conf &> $WORKDIR/logs/ingest-v1.stdout &
INGESTPID=$!

echo "Starting shard service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/shard-v1.logging.xml -classpath "$DESKTOP_ASSEMBLY" com.precog.shard.desktop.DesktopShardServer --configFile "$WORKDIR"/configs/shard-v1.conf &> $WORKDIR/logs/shard-v1.stdout &
SHARDPID=$!

# Let the ingest/shard services startup in parallel
wait_until_port_open $INGEST_PORT
wait_until_port_open $SHARD_PORT

cat > $WORKDIR/ports.txt <<EOF
KAFKA_LOCAL_PORT=$KAFKA_LOCAL_PORT
KAFKA_GLOBAL_PORT=$KAFKA_GLOBAL_PORT
ZOOKEEPER_PORT=$ZOOKEEPER_PORT
INGEST_PORT=$INGEST_PORT
SHARD_PORT=$SHARD_PORT
EOF

echo "Startup complete, running in $WORKDIR"

echo "============================================================"
echo "Base path: $WORKDIR"
cat <<EOF
KAFKA_LOCAL_PORT:  $KAFKA_LOCAL_PORT
KAFKA_GLOBAL_PORT: $KAFKA_GLOBAL_PORT
ZOOKEEPER_PORT:    $ZOOKEEPER_PORT
INGEST_PORT:       $INGEST_PORT
SHARD_PORT:        $SHARD_PORT
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

TESTJSON="n100k.json"
TESTURL="http://ops.reportgrid.com.s3.amazonaws.com/datasets/$TESTJSON"

function download_testjson() {
    echo "downloading json"
    wget $TESTURL
    if [ $? -ne 0 ]; then
        echo "Failed to download $TESTURL" >&2
        exit 3
    fi
    echo "done"
}

if [ -n "$TESTQUIT" ]; then

    echo "trying??"
    ( [ -r $TESTJSON ] && [ `count_lines $TESTJSON` -eq 100000 ] ) || download_testjson
    echo "what????"

    echo ";;; ingesting $TESTJSON"
    curl -o /dev/null -v \
      -H 'Content-Type: application/json' \
      --data-bin "@$TESTJSON" \
      "http://localhost:$INGEST_PORT/sync/fs/$ACCOUNTID/xyz?apiKey=$ACCOUNTTOKEN"

    echo ";;; polling for rows via count()"
    wait_til_nonzero 60

    trap EXIT

    if [ $? -eq 0 ]; then
        echo ";;; ingest rows detected--killing service now!"
        kill -9 $INGESTPID
        kill -9 $SHARDPID
        kill -9 $ACCOUNTSPID
        kill -9 $JOBSPID
        kill -9 $AUTHPID
        on_exit
        exit 0
    else
        echo ";;; no rows ingested after 60s--failed!"
        on_exit
        exit 1
    fi
fi

if [ -n "$TESTRESUME" ]; then
    NROWS=`wc -l $TESTJSON | awk '{print $1}'`
    wait_til_n_rows $NROWS 60
    trap EXIT
    echo ";;; verifying that shard resumes ingest"
    if [ $? -eq 0 ]; then
        echo ";;; resume succeeded (found $NROWS rows)"
        echo ";;; ok"
        on_exit
        exit 0
    else
        echo ";;; resume failed (timed out before seeing $NROWS rows)"
        echo ";;; ERROR!"
        on_exit 1
        exit 1
    fi
fi

# Wait forever until the user Ctrl-C's the system
while true; do sleep 30; done
