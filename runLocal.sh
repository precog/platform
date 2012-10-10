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
cd -P -- "$(dirname -- "${dst}")" &> /dev/null && echo "$(pwd -P)/$(basename -- "${dst}")"
}

function wait_until_port_open () {
    while ! netstat -an | grep $1 > /dev/null; do
        sleep 1
    done
}

BASEDIR=$(path-canonical-simple `dirname $0`)

VERSION=`grep "version :=" project/Build.scala | sed 's/.*"\(.*\)".*/\1/'`
INGEST_ASSEMBLY=$BASEDIR/ingest/target/ingest-assembly-$VERSION.jar
AUTH_ASSEMBLY=$BASEDIR/auth/target/auth-assembly-$VERSION.jar
ACCOUNTS_ASSEMBLY=$BASEDIR/accounts/target/accounts-assembly-$VERSION.jar
SHARD_ASSEMBLY=$BASEDIR/shard/target/shard-assembly-$VERSION.jar
YGGDRASIL_ASSEMBLY=$BASEDIR/yggdrasil/target/yggdrasil-assembly-$VERSION.jar

if [ ! -f $INGEST_ASSEMBLY -o ! -f $SHARD_ASSEMBLY -o ! -f $YGGDRASIL_ASSEMBLY -o ! -f $AUTH_ASSEMBLY -o ! -f $ACCOUNTS_ASSEMBLY ]; then
    echo "Ingest, shard, auth, accounts and yggdrasil assemblies are required before running. Please build and re-run."
    exit 1
fi

# Make sure we have the tools we need on OSX
if [ `uname` == "Darwin" ]; then
    echo "Running on OSX"
    alias tar="/usr/bin/gnutar"
    MONGOURL="http://fastdl.mongodb.org/osx/mongodb-osx-x86_64-2.2.0.tgz"
else
    MONGOURL="http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.2.0.tgz"
fi

# Parse opts to determine settings
while getopts "d:l" opt; do
    case $opt in
        d) 
            WORKDIR=$OPTARG
            ;;

        l)
            DONTCLEAN=1
            ;;

        \?)
            echo <<EOF
Usage: `basename $0` [-l] [-d <work directory>]"
  -l: If a temp workdir is used, don't clean up afterward
  -d: Use the provided workdir
EOF
            exit 1
            ;;
    esac
done


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
    wget -nd -q -r -l 1 -A tar.gz http://mirrors.gigenet.com/apache/zookeeper/current/ || { 
        echo "Failed to download zookeeper"
        exit 3 
    }
    popd > /dev/null
}

(exists $ARTIFACTDIR/kafka* && echo "  Kafka exists") || {
    echo "Downloading current Kafka artifact"
    pushd $ARTIFACTDIR > /dev/null
    wget -nd -q http://s3.amazonaws.com/ops.reportgrid.com/kafka/kafka-0.7.5.zip || { 
        echo "Failed to download kafka"
        exit 3 
    }
    popd > /dev/null
}

(exists $ARTIFACTDIR/mongo* && echo "  Mongo exists") || {
    echo "Downloading current Mongo artifact"
    pushd $ARTIFACTDIR > /dev/null
    wget -nd -q $MONGOURL || { 
        echo "Failed to download kafka"
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
        echo "Couldn't create temp workdir! ($WORKDIR)"
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

    if [ "$DONTCLEAN" != "1" ]; then
        echo "Cleaning up temp work dir"
        rm -rf $WORKDIR
    fi

    echo "Shutdown complete"
}

trap on_exit EXIT


# Get zookeeper up and running first
pushd $ZKBASE > /dev/null
tar --strip-components=1 --exclude='docs*' --exclude='src*' --exclude='dist-maven*' --exclude='contrib*' --exclude='recipes*' -xvzf $ARTIFACTDIR/zookeeper* > /dev/null 2>&1 || {
    echo "Failed to unpack zookeeper"
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
    echo "Failed to unpack kafka"
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
$MONGOBASE/bin/mongod --dbpath $MONGODATA &
MONGOPID=$!

wait_until_port_open 27017

if [ ! -e $WORKDIR/root_token.json ]; then
    java $REBEL_OPTS -jar $YGGDRASIL_ASSEMBLY tokens -d dev_auth_v1 -n "/" -a "Local test" -r "Unused" || exit 3
    echo 'db.tokens.find({}, {"tid":1})' | $MONGOBASE/bin/mongo dev_auth_v1 > $WORKDIR/root_token.json || {
        echo "Error retrieving new root token"
        exit 3
    }
fi

TOKENID=`grep tid $WORKDIR/root_token.json | sed -e 's/.*"tid" : "\(.*\)".*/\1/'`
echo $TOKENID > $WORKDIR/root_token.txt

# Set up ingest and shard services
sed -e "s#/var/log#$WORKDIR/logs#" < $BASEDIR/ingest/configs/dev/dev-ingest-v1.conf > $WORKDIR/configs/ingest-v1.conf
sed -e "s#/var/log/precog#$WORKDIR/logs#" < $BASEDIR/ingest/configs/dev/dev-ingest-v1.logging.xml > $WORKDIR/configs/ingest-v1.logging.xml

sed -e "s#/var/log#$WORKDIR/logs#; s#/opt/precog/shard#$WORKDIR/shard-data#" < $BASEDIR/shard/configs/dev/shard-v1.conf > $WORKDIR/configs/shard-v1.conf
sed -e "s#/var/log/precog#$WORKDIR/logs#" < $BASEDIR/shard/configs/dev/shard-v1.logging.xml > $WORKDIR/configs/shard-v1.logging.xml

sed -e "s#/var/log#$WORKDIR/logs#" < $BASEDIR/auth/configs/dev/dev-auth-v1.conf > $WORKDIR/configs/auth-v1.conf
sed -e "s#/var/log/precog#$WORKDIR/logs#" < $BASEDIR/auth/configs/dev/dev-auth-v1.logging.xml > $WORKDIR/configs/auth-v1.logging.xml

sed -e "s#/var/log#$WORKDIR/logs#; s/port = 80/port = 30062/; s#/security/v1/#/#; s/rootKey = .*/rootKey = \"$TOKENID\"/" < $BASEDIR/accounts/configs/dev/accounts-v1.conf > $WORKDIR/configs/accounts-v1.conf
sed -e "s#/var/log/precog#$WORKDIR/logs#" < $BASEDIR/accounts/configs/dev/accounts-v1.logging.xml > $WORKDIR/configs/accounts-v1.logging.xml

cd $BASEDIR

# Prior to ingest startup, we need to set an initial checkpoint if it's not already there
if [ ! -e $WORKDIR/initial_checkpoint.json ]; then
    java $REBEL_OPTS -jar $YGGDRASIL_ASSEMBLY zk -uc "/precog-dev/shard/checkpoint/`hostname`:{\"offset\":0, \"messageClock\":[]}" || {
        echo "Couldn't set initial checkpoint!"
        exit 3
    }
    touch $WORKDIR/initial_checkpoint.json
fi

echo "Starting ingest service"
java $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/ingest-v1.logging.xml -jar $INGEST_ASSEMBLY --configFile $WORKDIR/configs/ingest-v1.conf &
INGESTPID=$!

echo "Starting shard service"
echo java $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/shard-v1.logging.xml -jar $SHARD_ASSEMBLY --configFile $WORKDIR/configs/shard-v1.conf
java $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/shard-v1.logging.xml -jar $SHARD_ASSEMBLY --configFile $WORKDIR/configs/shard-v1.conf &
SHARDPID=$!

echo "Starting accounts service"
java $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/accounts-v1.logging.xml -jar $ACCOUNTS_ASSEMBLY --configFile $WORKDIR/configs/accounts-v1.conf &
ACCOUNTSPID=$!

echo "Starting auth service"
java $REBEL_OPTS -Dlogback.configurationFile=$WORKDIR/configs/auth-v1.logging.xml -jar $AUTH_ASSEMBLY --configFile $WORKDIR/configs/auth-v1.conf &
AUTHPID=$!

# Let the ingest//auth/accounts/shard services startup in parallel
wait_until_port_open 30060
wait_until_port_open 30062
wait_until_port_open 30064
wait_until_port_open 30070

echo "Startup complete, running in $WORKDIR"

TOKENID=`grep tid $WORKDIR/root_token.json | sed -e 's/.*"tid" : "\(.*\)".*/\1/'`
echo "============================================================"
echo "Root token: $TOKENID"
echo "Base path: $WORKDIR"
echo "============================================================"

# Wait forever until the user Ctrl-C's the system
while true; do sleep 30; done
