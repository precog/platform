#!/bin/bash

cd $(dirname $0)

. common.sh

if [ ! -d $WORKDIR ]; then
    echo "Please run ./setup.sh"
    exit 1
fi

# Shutdown hook
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

    if is_running $KFGLOBALPID; then
        echo "Stopping kafka..."
        # Kafka is somewhat of a pain, since the Java process daemonizes from within the startup script. That means that killing the script detaches
        # the Java process, leaving it running. Instead, we kill all child processes
        for pid in `ps -o pid,ppid | awk -v PID=$KFGLOBALPID '$2==PID{print $1}'`; do kill $pid; done
        wait $KFGLOBALPID
    fi

    if is_running $KFLOCALPID; then
        for pid in `ps -o pid,ppid | awk -v PID=$KFLOCALPID '{ if($2 == PID) print $1}'`; do kill $pid; done
        wait $KFLOCALPID
    fi

    echo "Stopping zookeeper..."
    cd $ZKBASE/bin
    ./zkServer.sh stop

    echo "Shutdown complete"
}

trap on_exit EXIT


# Start Zookeeper
cd $ZKBASE/bin
./zkServer.sh start &> $WORKDIR/logs/zookeeper.stdout
wait_until_port_open $ZOOKEEPER_PORT

# Kafka
$KFBASE/bin/kafka-server-start.sh $KFBASE/config/server-global.properties &> $WORKDIR/logs/kafka-global.stdout &
KFGLOBALPID=$!
wait_until_port_open $KAFKA_GLOBAL_PORT

$KFBASE/bin/kafka-server-start.sh $KFBASE/config/server-local.properties &> $WORKDIR/logs/kafka-local.stdout &
KFLOCALPID=$!
wait_until_port_open $KAFKA_LOCAL_PORT

# Auth, Accounts and Jobs
echo "Starting auth service on $AUTH_PORT"
$JAVA -Dlogback.configurationFile="$WORKDIR"/configs/auth-v1.logging.xml -jar "$AUTH_ASSEMBLY" --configFile "$WORKDIR"/configs/auth-v1.conf &> $WORKDIR/logs/auth-v1.stdout &
AUTHPID=$!

echo "Starting accounts service on $ACCOUNTS_PORT"
$JAVA -Dlogback.configurationFile="$WORKDIR"/configs/accounts-v1.logging.xml -jar "$ACCOUNTS_ASSEMBLY" --configFile "$WORKDIR"/configs/accounts-v1.conf &> $WORKDIR/logs/accounts-v1.stdout &
ACCOUNTSPID=$!

echo "Starting jobs service on $JOBS_PORT"
$JAVA -Dlogback.configurationFile="$WORKDIR"/configs/jobs-v1.logging.xml -jar "$JOBS_ASSEMBLY" --configFile "$WORKDIR"/configs/jobs-v1.conf &> $WORKDIR/logs/jobs-v1.stdout &
JOBSPID=$!

wait_until_port_open $AUTH_PORT
wait_until_port_open $ACCOUNTS_PORT
wait_until_port_open $JOBS_PORT

# Ingest and Shard
echo "Starting ingest service on $INGEST_PORT"
$JAVA -Dlogback.configurationFile="$WORKDIR"/configs/ingest-v2.logging.xml -jar "$INGEST_ASSEMBLY" --configFile "$WORKDIR"/configs/ingest-v2.conf &> $WORKDIR/logs/ingest-v2.stdout &
INGESTPID=$!

echo "Starting shard service on $SHARD_PORT"
$JAVA -Dlogback.configurationFile="$WORKDIR"/configs/shard-v2.logging.xml -jar "$SHARD_ASSEMBLY" --configFile "$WORKDIR"/configs/shard-v2.conf &> $WORKDIR/logs/shard-v2.stdout &
SHARDPID=$!

wait_until_port_open $INGEST_PORT
wait_until_port_open $SHARD_PORT


# Echo details
echo "Startup complete, running in $WORKDIR"


# Wait forever until the user Ctrl-C's the system
echo "Press Ctrl-C to exit"
while true; do sleep 30; done
