#!/bin/bash

cd "$(dirname $0)"

# stolen shamelessly from start-shard.sh
port_is_open() {
	netstat -an | egrep "[\.:]$1[[:space:]]+.*LISTEN" > /dev/null
}

wait_until_port_open() {
    while port_is_open $1; do
        sleep 1
    done
}

SUCCESS=0
FAILEDTARGETS=""
MONGOPORT=27117

function run_sbt() {
    sbt -mem 2048 -J-Dsbt.log.noformat=true $@
    if [[ $? != 0 ]]; then
        SUCCESS=1
        for TARGET in $@; do
            FAILEDTARGETS="$TARGET $FAILEDTARGETS"
        done
    fi
}

while getopts ":m:s" opt; do
    case $opt in
        m)
            echo "Overriding default mongo port with $OPTARG"
            MONGOPORT=$OPTARG
            ;;
        s)
            SKIPSETUP=1
            ;;
        \?)
            echo "Usage: `basename $0` [-s] [-m <mongo port>]"
            echo "  -s: Skip clean/compile setup steps"
            echo "  -m: Use the specified port for mongo"
            exit 1
            ;;
    esac
done

if [ -z "$SKIPSETUP" ]; then
    (cd quirrel && ln -fs ../../research/quirrel/examples)

    run_sbt clean
    
    run_sbt compile

    run_sbt test:compile

    run_sbt yggdrasil/assembly
else
    echo "Skipping clean/compile"
fi

# For the runs, we don't want to terminate early if a particular project fails
set +e
for PROJECT in util common daze auth accounts ragnarok ingest bytecode quirrel muspelheim yggdrasil shard pandora; do
  run_sbt "$PROJECT/test"
done

if [ $SUCCESS -eq 0 ]; then
  run_sbt accounts/assembly auth/assembly ingest/assembly yggdrasil/assembly shard/assembly
fi

if [ $SUCCESS -eq 0 ]; then
	wait_until_port_open $MONGOPORT
	
	shard/test.sh -m $MONGOPORT
	SUCCESS=$(($SUCCESS || $?))
fi

# re-enable errexit
set -e

# For the triggers
if [ $SUCCESS -eq 0 ]; then
  echo "Finished: SUCCESS"
else
  echo "Finished: FAILURE"
  echo "Failed targets: $FAILEDTARGETS"
fi

exit $SUCCESS
