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

function run_sbt() {
    sbt -mem 2048 $OPTIMIZE -J-Dsbt.log.noformat=true $@
    if [[ $? != 0 ]]; then
        SUCCESS=1
        for TARGET in $@; do
            FAILEDTARGETS="$TARGET $FAILEDTARGETS"
        done
    fi
}

while getopts ":m:sao" opt; do
    case $opt in
        s)
            SKIPSETUP=1
            ;;
        a)
            SKIPTEST=1
            ;;
        o)
            OPTIMIZE="-J-Dcom.precog.build.optimize=true"
            ;;
        \?)
            echo "Usage: `basename $0` [-a] [-o] [-s] [-m <mongo port>]"
            echo "  -a: Build assemdblies only"
            echo "  -o: Optimized build"
            echo "  -s: Skip clean/compile setup steps"
            exit 1
            ;;
    esac
done

if [ -z "$SKIPSETUP" ]; then
    echo "Running clean/compile"
    (cd quirrel && ln -fs ../../research/quirrel/examples)

    run_sbt clean
    
    run_sbt compile

    run_sbt test:compile
else
    echo "Skipping clean/compile"
fi

if [ ! -f "yggdrasil/target/yggdrasil-assembly-$(git describe).jar" ]; then
    run_sbt yggdrasil/assembly
fi

# For the runs, we don't want to terminate early if a particular project fails
set +e

if [ -z "$SKIPTEST" ]; then
    for PROJECT in util common daze auth accounts ragnarok heimdall ingest bytecode quirrel muspelheim yggdrasil shard pandora mongo; do
        run_sbt "$PROJECT/test"
    done
fi

if [ $SUCCESS -eq 0 ]; then
    echo "Building assemblies"
    run_sbt accounts/assembly auth/assembly ingest/assembly yggdrasil/assembly shard/assembly heimdall/assembly
fi

if [ $SUCCESS -eq 0 -a -z "$SKIPTEST" ]; then
    echo "Running full shard test"
    shard/test.sh 
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
