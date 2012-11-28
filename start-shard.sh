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

MONGOPORT=27017

# Parse opts to determine settings
while getopts ":d:lbm:" opt; do
    case $opt in
        d) 
            WORKDIR=$OPTARG
            ;;
        l)
            DONTCLEAN=1
            ;;
        b)
            BUILDMISSING=1
            ;;
        m)
            echo "Overriding default mongo port with $OPTARG"
            MONGOPORT=$OPTARG
            ;;

        \?)
            echo "Usage: `basename $0` [-l] [-d <work directory>] [-m <mongo port>]"
            echo "  -l: If a temp workdir is used, don't clean up afterward"
            echo "  -d: Use the provided workdir"
            echo "  -m: Use the specified port for mongo"
            echo "  -b: Build missing artifacts prior to run (depends on sbt in path)"
            exit 1
            ;;
    esac
done

# Taken from http://blog.publicobject.com/2006/06/canonical-path-of-file-in-bash.html
function path-canonical-simple() {
local dst="${1}"
cd -P -- "$(dirname -- "${dst}")" &> /dev/null && echo "$(pwd -P)/$(basename -- "${dst}")" | sed 's#/\.##'
}

function port_is_open() {
   netstat -an | egrep "[\.:]$1[[:space:]]+.*LISTEN" > /dev/null
}

function wait_until_port_open () {
    while ! port_is_open $1; do
        sleep 1
    done
}

BASEDIR=$(path-canonical-simple `dirname $0`)

VERSION=`git describe`
INGEST_ASSEMBLY=$BASEDIR/ingest/target/ingest-assembly-$VERSION.jar
AUTH_ASSEMBLY=$BASEDIR/auth/target/auth-assembly-$VERSION.jar
ACCOUNTS_ASSEMBLY=$BASEDIR/accounts/target/accounts-assembly-$VERSION.jar
SHARD_ASSEMBLY=$BASEDIR/shard/target/shard-assembly-$VERSION.jar
YGGDRASIL_ASSEMBLY=$BASEDIR/yggdrasil/target/yggdrasil-assembly-$VERSION.jar

GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:-CMSIncrementalPacing -XX:CMSIncrementalDutyCycle=100"

JAVA="java $GC_OPTS"

# pre-flight checks to make sure we have everything we need, and to make sure there aren't any conflicting daemons running
MISSING_ARTIFACTS=""
for ASM in $INGEST_ASSEMBLY $SHARD_ASSEMBLY $YGGDRASIL_ASSEMBLY $AUTH_ASSEMBLY $ACCOUNTS_ASSEMBLY; do
    if [ ! -f $ASM ]; then
        if [ -n "$BUILDMISSING" ]; then
            # Darn you, bash! zsh can do this in one go, a la ${$(basename $ASM)%%-*}
            BUILDTARGETBASE=$(basename $ASM)
            BUILDTARGET=${BUILDTARGETBASE%%-*}/assembly
            echo "Building $BUILDTARGET"
            sbt $BUILDTARGET || {
                echo "Failed to build $BUILDTARGET!" >&2
                exit 1
            }
        else
            MISSING_ARTIFACTS="$MISSING_ARTIFACTS $ASM"
        fi
    fi
done


if [ -n "$MISSING_ARTIFACTS" ]; then
    echo "Up-to-date ingest, shard, auth, accounts and yggdrasil assemblies are required before running. Please build and re-run, or run with the -b flag." >&2
    for ASM in $MISSING_ARTIFACTS; do
        echo "  missing `basename $ASM`" >&2
    done
    exit 1
fi

declare -a service_ports
service_ports[9082]="Kafka Local"
service_ports[9092]="Kafka Global"
service_ports[$MONGOPORT]="MongoDB"
service_ports[30060]="Ingest"
service_ports[30062]="Auth"
service_ports[30064]="Accounts"
service_ports[30070]="Shard"

for PORT in 9082 9092 $MONGOPORT 30060 30062 30064 30070; do
    if port_is_open $PORT; then
        echo "You appear to already have a conflicting ${service_ports[$PORT]} service running on port $PORT" >&2
        if [[ $PORT == $MONGOPORT ]]; then
            echo "You can use the -m flag to override the mongo port" >&2
        fi
        exit 1
    fi
done

# Make sure we have the tools we need on OSX
if [ `uname` == "Darwin" ]; then
    echo "Running on OSX"
    alias tar="/usr/bin/gnutar"
    MONGOURL="http://fastdl.mongodb.org/osx/mongodb-osx-x86_64-2.2.0.tgz"
else
    MONGOURL="http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.2.0.tgz"
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

[ -d $ARTIFACTDIR ] || mkdir $ARTIFACTDIR

(exists $ARTIFACTDIR/zookeeper* && echo "  ZooKeeper exists") || {
    echo "Downloading current ZooKeeper artifact"
    pushd $ARTIFACTDIR > /dev/null
    wget -nd -q http://ops.reportgrid.com.s3.amazonaws.com/zookeeper/zookeeper-3.4.3.tar.gz || { 
        echo "Failed to download zookeeper" >&2
        exit 3
    }
    popd > /dev/null
}

(exists $ARTIFACTDIR/kafka* && echo "  Kafka exists") || {
    echo "Downloading current Kafka artifact"
    pushd $ARTIFACTDIR > /dev/null
    wget -nd -q http://s3.amazonaws.com/ops.reportgrid.com/kafka/kafka-0.7.5.zip || { 
        echo "Failed to download kafka" >&2
        exit 3
    }
    popd > /dev/null
}

(exists $ARTIFACTDIR/mongo* && echo "  Mongo exists") || {
    echo "Downloading current Mongo artifact"
    pushd $ARTIFACTDIR > /dev/null
    wget -nd -q $MONGOURL || { 
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
ZKBASE=$WORKDIR/zookeeper
ZKDATA=$WORKDIR/zookeeper-data

KFBASE=$WORKDIR/kafka
KFGLOBALDATA=$WORKDIR/kafka-global
KFLOCALDATA=$WORKDIR/kafka-local

MONGOBASE=$WORKDIR/mongo
MONGODATA=$WORKDIR/mongodata

rm -rf $ZKBASE $KFBASE
mkdir -p $ZKBASE $KFBASE $ZKDATA $MONGOBASE $MONGODATA $WORKDIR/{configs,logs,shard-data/data}
  
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

    if is_running $ACCOUNTSPID; then
        echo "Stopping accounts..."
        kill $ACCOUNTSPID
        wait $ACCOUNTSPID
    fi

    if is_running $AUTHPID; then
        echo "Stopping auth..."
        kill $AUTHPID
        wait $AUTHPID
    fi

    if is_running $MONGOPID; then
        echo "Stopping mongo..."
        kill $MONGOPID
        wait $MONGOPID
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
        rm -rf $WORKDIR
    fi

    echo "Shutdown complete"
}

trap on_exit EXIT


# Get zookeeper up and running first
pushd $ZKBASE > /dev/null
tar --strip-components=1 --exclude='docs*' --exclude='src*' --exclude='dist-maven*' --exclude='contrib*' --exclude='recipes*' -xvzf $ARTIFACTDIR/zookeeper* > /dev/null 2>&1 || {
    echo "Failed to unpack zookeeper" >&2
    exit 3
}
popd > /dev/null

# Copy in a simple config
echo "# the directory where the snapshot is stored." >> $ZKBASE/conf/zoo.cfg
echo "dataDir=$ZKDATA" >> $ZKBASE/conf/zoo.cfg
echo "# the port at which the clients will connect" >> $ZKBASE/conf/zoo.cfg
echo "clientPort=2181" >> $ZKBASE/conf/zoo.cfg

# Start it up!
cd $ZKBASE/bin
./zkServer.sh start

# Now, start global and local kafkas
cd $WORKDIR
unzip $ARTIFACTDIR/kafka* > /dev/null || {
    echo "Failed to unpack kafka" >&2
    exit 3
}

# Transform the provided config into global and local configs
cd $WORKDIR/kafka/config
sed -e "s#log.dir=.*#log.dir=$KFGLOBALDATA#; s/port=.*/port=9092/" < server.properties > server-global.properties
sed -e "s#log.dir=.*#log.dir=$KFLOCALDATA#; s/port=.*/port=9082/; s/enable.zookeeper=.*/enable.zookeeper=false/" < server.properties > server-local.properties

chmod +x $KFBASE/bin/kafka-server-start.sh

# Start up kafka instances using the global and local configs
$KFBASE/bin/kafka-server-start.sh $KFBASE/config/server-global.properties &
KFGLOBALPID=$!

$KFBASE/bin/kafka-server-start.sh $KFBASE/config/server-local.properties &
KFLOCALPID=$!

wait_until_port_open 9092
wait_until_port_open 9082

echo "Kafka Global = $KFGLOBALPID"
echo "Kafka Local = $KFLOCALPID"

# Start up mongo and set test token
cd $MONGOBASE
tar --strip-components=1 -xvzf $ARTIFACTDIR/mongo* &> /dev/null
$MONGOBASE/bin/mongod --port $MONGOPORT --dbpath $MONGODATA --nojournal --nounixsocket --noauth --noprealloc &
MONGOPID=$!

wait_until_port_open $MONGOPORT

if [ ! -e $WORKDIR/root_token.json ]; then
    echo "Retrieving new root token"
    $JAVA $REBEL_OPTS -jar $YGGDRASIL_ASSEMBLY tokens -s "localhost:$MONGOPORT" -d dev_auth_v1 -r | tail -n 1 > $WORKDIR/root_token.txt || {
        echo "Error retrieving new root token" >&2
        exit 3
    }
fi

TOKENID=`cat $WORKDIR/root_token.txt`

# Set up ingest and shard services
sed -e "s#/var/log#$WORKDIR/logs#; s#\[\"localhost\"\]#\[\"localhost:$MONGOPORT\"\]#; s#/accounts/v1/#/#" < $BASEDIR/ingest/configs/dev/dev-ingest-v1.conf > $WORKDIR/configs/ingest-v1.conf || echo "Failed to update ingest config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < $BASEDIR/ingest/configs/dev/dev-ingest-v1.logging.xml > $WORKDIR/configs/ingest-v1.logging.xml

sed -e "s#/var/log#$WORKDIR/logs#; s#\[\"localhost\"\]#\[\"localhost:$MONGOPORT\"\]#; s#/opt/precog/shard#$WORKDIR/shard-data#" < $BASEDIR/shard/configs/dev/shard-v1.conf > $WORKDIR/configs/shard-v1.conf || echo "Failed to update shard config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < $BASEDIR/shard/configs/dev/shard-v1.logging.xml > $WORKDIR/configs/shard-v1.logging.xml

sed -e "s#/var/log#$WORKDIR/logs#; s#\[\"localhost\"\]#\[\"localhost:$MONGOPORT\"\]#" < $BASEDIR/auth/configs/dev/dev-auth-v1.conf > $WORKDIR/configs/auth-v1.conf || echo "Failed to update auth config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < $BASEDIR/auth/configs/dev/dev-auth-v1.logging.xml > $WORKDIR/configs/auth-v1.logging.xml

sed -e "s!/var/log!$WORKDIR/logs!; s#\[\"localhost\"\]#\[\"localhost:$MONGOPORT\"\]#; s/port = 80/port = 30062/; s#/security/v1/#/#; s/rootKey = .*/rootKey = \"$TOKENID\"/" < $BASEDIR/accounts/configs/dev/accounts-v1.conf > $WORKDIR/configs/accounts-v1.conf || echo "Failed to update accounts config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < $BASEDIR/accounts/configs/dev/accounts-v1.logging.xml > $WORKDIR/configs/accounts-v1.logging.xml

cd $BASEDIR

# Prior to ingest startup, we need to set an initial checkpoint if it's not already there
if [ ! -e $WORKDIR/initial_checkpoint.json ]; then
    $JAVA $REBEL_OPTS -jar $YGGDRASIL_ASSEMBLY zk -uc "/precog-dev/shard/checkpoint/`hostname`:{\"offset\":0, \"messageClock\":[]}" || {
        echo "Couldn't set initial checkpoint!" >&2
        exit 3
    }
    touch $WORKDIR/initial_checkpoint.json
fi

echo "Starting auth service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/auth-v1.logging.xml -jar $AUTH_ASSEMBLY --configFile $WORKDIR/configs/auth-v1.conf &
AUTHPID=$!

echo "Starting accounts service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/accounts-v1.logging.xml -jar $ACCOUNTS_ASSEMBLY --configFile $WORKDIR/configs/accounts-v1.conf &
ACCOUNTSPID=$!

wait_until_port_open 30062
wait_until_port_open 30064

# Now we need an actual account to use for testing
if [ ! -e $WORKDIR/account_token.txt ]; then
    echo "Creating test account"
    ACCOUNTID=$(set -e; curl -S -s -H 'Content-Type: application/json' -d '{"email":"operations@precog.com","password":"1234"}' http://localhost:30064/accounts/ | sed 's/.*\([0-9]\{10\}\).*/\1/')
    echo "Created account: $ACCOUNTID"
    echo $ACCOUNTID > $WORKDIR/account_id.txt
    ACCOUNTTOKEN=$(set -e; curl -S -s -u 'operations@precog.com:1234' -H 'Content-Type: application/json' -G "http://localhost:30064/accounts/$ACCOUNTID" | grep apiKey | sed 's/.*apiKey"[^"]*"\([^"]*\)".*/\1/')
    echo "Account token is $ACCOUNTTOKEN"
    echo $ACCOUNTTOKEN > $WORKDIR/account_token.txt
fi

echo "Starting ingest service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/ingest-v1.logging.xml -jar $INGEST_ASSEMBLY --configFile $WORKDIR/configs/ingest-v1.conf &
INGESTPID=$!

echo "Starting shard service"
echo $JAVA $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/shard-v1.logging.xml -jar $SHARD_ASSEMBLY --configFile $WORKDIR/configs/shard-v1.conf
$JAVA $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/shard-v1.logging.xml -jar $SHARD_ASSEMBLY --configFile $WORKDIR/configs/shard-v1.conf &
SHARDPID=$!

# Let the ingest/shard services startup in parallel
wait_until_port_open 30060
wait_until_port_open 30070

echo "Startup complete, running in $WORKDIR"

echo "============================================================"
echo "Root token: $TOKENID"
echo "Test account ID: $ACCOUNTID"
echo "Test account token: $ACCOUNTTOKEN"
echo "Base path: $WORKDIR"
echo "============================================================"

# Wait forever until the user Ctrl-C's the system
while true; do sleep 30; done
