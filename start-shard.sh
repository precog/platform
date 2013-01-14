#!/bin/bash

# Parse opts to determine settings
while getopts ":d:lb" opt; do
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
        \?)
            echo "Usage: `basename $0` [-l] [-d <work directory>]"
            echo "  -l: If a temp workdir is used, don't clean up afterward"
            echo "  -d: Use the provided workdir"
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

BASEDIR=$(path-canonical-simple `dirname $0`)

VERSION=`git describe`
INGEST_ASSEMBLY="$BASEDIR"/ingest/target/ingest-assembly-$VERSION.jar
AUTH_ASSEMBLY="$BASEDIR"/auth/target/auth-assembly-$VERSION.jar
ACCOUNTS_ASSEMBLY="$BASEDIR"/accounts/target/accounts-assembly-$VERSION.jar
JOBS_ASSEMBLY="$BASEDIR"/heimdall/target/heimdall-assembly-$VERSION.jar
SHARD_ASSEMBLY="$BASEDIR"/shard/target/shard-assembly-$VERSION.jar
YGGDRASIL_ASSEMBLY="$BASEDIR"/yggdrasil/target/yggdrasil-assembly-$VERSION.jar

GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -XX:-CMSIncrementalPacing -XX:CMSIncrementalDutyCycle=100"

JAVA="java $GC_OPTS"

# pre-flight checks to make sure we have everything we need, and to make sure there aren't any conflicting daemons running
MISSING_ARTIFACTS=""
for ASM in "$INGEST_ASSEMBLY" "$SHARD_ASSEMBLY" "$YGGDRASIL_ASSEMBLY" "$AUTH_ASSEMBLY" "$ACCOUNTS_ASSEMBLY" "$JOBS_ASSEMBLY"; do
    if [ ! -f "$ASM" ]; then
        if [ -n "$BUILDMISSING" ]; then
            # Darn you, bash! zsh can do this in one go, a la ${$(basename $ASM)%%-*}
            BUILDTARGETBASE=$(basename "$ASM")
            BUILDTARGET="${BUILDTARGETBASE%%-*}/assembly"
            echo "Building $BUILDTARGET"
            sbt "$BUILDTARGET" || {
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

(exists "$ARTIFACTDIR"/mongo* && echo "  Mongo exists") || {
    echo "Downloading current Mongo artifact"
    pushd "$ARTIFACTDIR" > /dev/null
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
ZKBASE="$WORKDIR"/zookeeper
ZKDATA="$WORKDIR"/zookeeper-data

KFBASE="$WORKDIR"/kafka
KFGLOBALDATA="$WORKDIR"/kafka-global
KFLOCALDATA="$WORKDIR"/kafka-local

MONGOBASE="$WORKDIR"/mongo
MONGODATA="$WORKDIR"/mongodata

rm -rf $ZKBASE $KFBASE
mkdir -p $ZKBASE $KFBASE $ZKDATA $MONGOBASE $MONGODATA "$WORKDIR"/{configs,logs,shard-data/data,shard-data/archive,shard-data/scratch,shard-data/ingest_failures}
  
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

    if is_running $JOBSPID; then
        echo "Stopping jobs..."
        kill $JOBSPID
        wait $JOBSPID
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

# Start it up!
echo "Starting zookeeper on port $ZOOKEEPER_PORT"
cd $ZKBASE/bin
./zkServer.sh start

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
$KFBASE/bin/kafka-server-start.sh $KFBASE/config/server-global.properties &
KFGLOBALPID=$!

wait_until_port_open $KAFKA_GLOBAL_PORT

KAFKA_LOCAL_PORT=$(random_port "Kafka local")
sed -e "s#log.dir=.*#log.dir=$KFLOCALDATA#; s/port=.*/port=$KAFKA_LOCAL_PORT/; s/enable.zookeeper=.*/enable.zookeeper=false/; s/zk.connect=localhost:2181/zk.connect=localhost:$ZOOKEEPER_PORT/" < server.properties > server-local.properties
$KFBASE/bin/kafka-server-start.sh $KFBASE/config/server-local.properties &
KFLOCALPID=$!

wait_until_port_open $KAFKA_LOCAL_PORT

echo "Kafka Global = $KFGLOBALPID"
echo "Kafka Local = $KFLOCALPID"

MONGO_PORT=$(random_port "Mongo")

# Start up mongo and set test token
cd $MONGOBASE
tar --strip-components=1 -xvzf "$ARTIFACTDIR"/mongo* &> /dev/null
$MONGOBASE/bin/mongod --port $MONGO_PORT --dbpath $MONGODATA --nojournal --nounixsocket --noauth --noprealloc &
MONGOPID=$!

wait_until_port_open $MONGO_PORT

if [ ! -e "$WORKDIR"/root_token.txt ]; then
    echo "Retrieving new root token"
    $JAVA $REBEL_OPTS -jar "$YGGDRASIL_ASSEMBLY" tokens -s "localhost:$MONGO_PORT" -d dev_auth_v1 -c | tail -n 1 > "$WORKDIR"/root_token.txt || {
        echo "Error retrieving new root token" >&2
        exit 3
    }
fi

TOKENID=`cat "$WORKDIR"/root_token.txt`

# FIXME: There's a potential for collisions here because we're
# assigning before actually starting services, but proper ordering
# would make things a bit more complicated and with bash's RNG this is
# low-risk
INGEST_PORT=$(random_port "Ingest")
AUTH_PORT=$(random_port "Auth")
ACCOUNTS_PORT=$(random_port "Accounts")
JOBS_PORT=$(random_port "Jobs service")
SHARD_PORT=$(random_port "Shard")

# Set up ingest and shard services
sed -e "s#/var/log#$WORKDIR/logs#; s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#; s#/accounts/v1/#/#; s/rootKey = .*/rootKey = \"$TOKENID\"/; s/port = 9082/port = $KAFKA_LOCAL_PORT/; s/port = 9092/port = $KAFKA_GLOBAL_PORT/; s/connect = localhost:2181/connect = localhost:$ZOOKEEPER_PORT/; s/port = 30060/port = $INGEST_PORT/" < "$BASEDIR"/ingest/configs/dev/dev-ingest-v1.conf > "$WORKDIR"/configs/ingest-v1.conf || echo "Failed to update ingest config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/ingest/configs/dev/dev-ingest-v1.logging.xml > "$WORKDIR"/configs/ingest-v1.logging.xml

sed -e "s#/var/log#$WORKDIR/logs#; s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#; s#/opt/precog/shard#$WORKDIR/shard-data#; s/rootKey = .*/rootKey = \"$TOKENID\"/; s/port = 9092/port = $KAFKA_GLOBAL_PORT/; s/hosts = localhost:2181/hosts = localhost:$ZOOKEEPER_PORT/; s/port = 30070/port = $SHARD_PORT/; s/port = 30064/port = $ACCOUNTS_PORT/" < "$BASEDIR"/shard/configs/dev/shard-v1.conf > "$WORKDIR"/configs/shard-v1.conf || echo "Failed to update shard config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/shard/configs/dev/shard-v1.logging.xml > "$WORKDIR"/configs/shard-v1.logging.xml

sed -e "s#/var/log#$WORKDIR/logs#; s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#; s/rootKey = .*/rootKey = \"$TOKENID\"/; s/port = 30062/port = $AUTH_PORT/" < "$BASEDIR"/auth/configs/dev/dev-auth-v1.conf > "$WORKDIR"/configs/auth-v1.conf || echo "Failed to update auth config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/auth/configs/dev/dev-auth-v1.logging.xml > "$WORKDIR"/configs/auth-v1.logging.xml

sed -e "s!/var/log!$WORKDIR/logs!; s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#; s/port = 80/port = $AUTH_PORT/; s#/security/v1/#/#; s/rootKey = .*/rootKey = \"$TOKENID\"/; s/hosts = localhost:2181/hosts = localhost:$ZOOKEEPER_PORT/; s/port = 30064/port = $ACCOUNTS_PORT/" < "$BASEDIR"/accounts/configs/dev/accounts-v1.conf > "$WORKDIR"/configs/accounts-v1.conf || echo "Failed to update accounts config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/accounts/configs/dev/accounts-v1.logging.xml > "$WORKDIR"/configs/accounts-v1.logging.xml

sed -e "s!/var/log!$WORKDIR/logs!; s/port = 80/port = $AUTH_PORT/; s!/security/v1/!/!; s!\[\"localhost\"\]!\[\"localhost:$MONGO_PORT\"\]!; s/hosts = localhost:2181/hosts = localhost:$ZOOKEEPER_PORT/; s/port = 30066/port = $JOBS_PORT/" < "$BASEDIR"/heimdall/configs/dev/jobs-v1.conf > "$WORKDIR"/configs/jobs-v1.conf || echo "Failed to update jobs config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/heimdall/configs/dev/jobs-v1.logging.xml > "$WORKDIR"/configs/jobs-v1.logging.xml

cd "$BASEDIR"

# Prior to ingest startup, we need to set an initial checkpoint if it's not already there
if [ ! -e "$WORKDIR"/initial_checkpoint.json ]; then
    $JAVA $REBEL_OPTS -jar "$YGGDRASIL_ASSEMBLY" zk -z "localhost:$ZOOKEEPER_PORT" -uc "/precog-dev/shard/checkpoint/`hostname`:{\"offset\":0, \"messageClock\":[]}" || {
        echo "Couldn't set initial checkpoint!" >&2
        exit 3
    }
    touch "$WORKDIR"/initial_checkpoint.json
fi

echo "Starting auth service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/auth-v1.logging.xml -jar "$AUTH_ASSEMBLY" --configFile "$WORKDIR"/configs/auth-v1.conf &
AUTHPID=$!

echo "Starting accounts service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/accounts-v1.logging.xml -jar "$ACCOUNTS_ASSEMBLY" --configFile "$WORKDIR"/configs/accounts-v1.conf &
ACCOUNTSPID=$!

echo "Starting jobs service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/jobs-v1.logging.xml -jar "$JOBS_ASSEMBLY" --configFile "$WORKDIR"/configs/jobs-v1.conf &
JOBSPID=$!

wait_until_port_open $AUTH_PORT
wait_until_port_open $ACCOUNTS_PORT
wait_until_port_open $JOBS_PORT

# Now we need two accounts for testing: the root account and the test account
if [ ! -e "$WORKDIR"/account_token.txt ]; then
    echo "Creating root account"
    ROOTACCOUNTID=$(set -e; curl -S -s -H 'Content-Type: application/json' -d '{"email":"operations@precog.com","password":"1234"}' "http://localhost:$ACCOUNTS_PORT/accounts/" | sed 's/.*\([0-9]\{10\}\).*/\1/')
    echo "Created root account: $ROOTACCOUNTID"
    echo "Updating root account with prior root APIKey"
    echo -e "db.accounts.update({\"accountId\":\"$ROOTACCOUNTID\"},{\$set:{\"apiKey\":\"$TOKENID\"}})" | "$WORKDIR"/mongo/bin/mongo --port $MONGO_PORT accounts_v1
    echo "Update of root account complete"

    echo "Creating test account"
    ACCOUNTID=$(set -e; curl -S -s -H 'Content-Type: application/json' -d '{"email":"test@precog.com","password":"fooble"}' "http://localhost:$ACCOUNTS_PORT/accounts/" | sed 's/.*\([0-9]\{10\}\).*/\1/')
    echo "Created test account: $ACCOUNTID"
    echo $ACCOUNTID > "$WORKDIR"/account_id.txt
    ACCOUNTTOKEN=$(set -e; curl -S -s -u 'test@precog.com:fooble' -H 'Content-Type: application/json' -G "http://localhost:$ACCOUNTS_PORT/accounts/$ACCOUNTID" | grep apiKey | sed 's/.*apiKey"[^"]*"\([^"]*\)".*/\1/')
    echo "Account token is $ACCOUNTTOKEN"
    echo $ACCOUNTTOKEN > "$WORKDIR"/account_token.txt
else
    ACCOUNTID=$(cat "$WORKDIR"/account_id.txt)
    ACCOUNTTOKEN=$(cat "$WORKDIR"/account_token.txt)
fi

echo "Starting ingest service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/ingest-v1.logging.xml -jar "$INGEST_ASSEMBLY" --configFile "$WORKDIR"/configs/ingest-v1.conf &
INGESTPID=$!

echo "Starting shard service"
$JAVA $REBEL_OPTS -Dlogback.configurationFile="$WORKDIR"/configs/shard-v1.logging.xml -jar "$SHARD_ASSEMBLY" --configFile "$WORKDIR"/configs/shard-v1.conf &
SHARDPID=$!

# Let the ingest/shard services startup in parallel
wait_until_port_open $INGEST_PORT
wait_until_port_open $SHARD_PORT

cat > $WORKDIR/ports.txt <<EOF
MONGO_PORT=$MONGO_PORT
KAFKA_LOCAL_PORT=$KAFKA_LOCAL_PORT
KAFKA_GLOBAL_PORT=$KAFKA_GLOBAL_PORT
ZOOKEEPER_PORT=$ZOOKEEPER_PORT
INGEST_PORT=$INGEST_PORT
AUTH_PORT=$AUTH_PORT
ACCOUNTS_PORT=$ACCOUNTS_PORT
JOBS_PORT=$JOBS_PORT
SHARD_PORT=$SHARD_PORT
EOF

echo "Startup complete, running in $WORKDIR"

echo "============================================================"
echo "Root token: $TOKENID"
echo "Root account ID: $ROOTACCOUNTID"
echo "Test account ID: $ACCOUNTID"
echo "Test account token: $ACCOUNTTOKEN"
echo "Base path: $WORKDIR"
cat <<EOF
MONGO_PORT:        $MONGO_PORT
KAFKA_LOCAL_PORT:  $KAFKA_LOCAL_PORT
KAFKA_GLOBAL_PORT: $KAFKA_GLOBAL_PORT
ZOOKEEPER_PORT:    $ZOOKEEPER_PORT
INGEST_PORT:       $INGEST_PORT
AUTH_PORT:         $AUTH_PORT
ACCOUNTS_PORT:     $ACCOUNTS_PORT
JOBS_PORT:         $JOBS_PORT
SHARD_PORT:        $SHARD_PORT
EOF
echo "============================================================"

# Wait forever until the user Ctrl-C's the system
while true; do sleep 30; done
