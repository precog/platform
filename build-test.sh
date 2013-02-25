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
    echo "SBT Run: $@"
    sbt -mem 2048 -J-Xss2m $OPTIMIZE -J-Dsbt.log.noformat=true $@
    if [[ $? != 0 ]]; then
        SUCCESS=1
        for TARGET in $@; do
            FAILEDTARGETS="$TARGET $FAILEDTARGETS"
        done
    fi
}

while getopts ":m:saokcf" opt; do
    case $opt in
        k)
            SKIPCLEAN=1
            ;;
        s)
            SKIPSETUP=1
            ;;
        a)
            SKIPTEST=1
            ;;
        o)
            OPTIMIZE="-J-Dcom.precog.build.optimize=true"
            ;;
	c)
	    COVERAGE=1
	    ;;
	f)
	    FAILFAST=1
	    ;;
        \?)
            echo "Usage: `basename $0` [-a] [-o] [-s] [-k] [-m <mongo port>]"
            echo "  -a: Build assemdblies only"
            echo "  -o: Optimized build"
            echo "  -s: Skip all clean/compile setup steps"
            echo "  -k: Skip sbt clean step"
	    echo "  -c: Do code coverage"
	    echo "  -f: Fail fast if a module test fails"
            exit 1
            ;;
    esac
done

if [ -n "$COVERAGE" ]; then
	SCCT="scct:"
	SCCTTEST="scct-"
else
	SCCT=""
	SCCTTEST=""
fi

# enable errexit (jenkins does it, but not shell)
set -e

if [ -z "$SKIPSETUP" ]; then
    echo "Linking quirrel examples"
    (cd quirrel && ln -fs ../../research/quirrel/examples)

    [ -z "$SKIPCLEAN" ] && find . -type d -name target -prune -exec rm -fr {} \;
    [ -z "$SKIPCLEAN" ] && run_sbt clean
    
    run_sbt "${SCCT}compile"

    run_sbt "${SCCTTEST}test:compile"
else
    echo "Skipping clean/compile"
fi

if [ ! -f "yggdrasil/target/yggdrasil-assembly-$(git describe).jar" ]; then
    run_sbt yggdrasil/assembly
fi

# For the runs, we don't want to terminate early if a particular project fails
if [ -z "$FAILFAST" ]
then
	set +e
fi

if [ -z "$SKIPTEST" ]; then
    for PROJECT in util common daze auth accounts ragnarok heimdall ingest bytecode quirrel muspelheim yggdrasil shard pandora mongo jdbc; do
	run_sbt "$PROJECT/${SCCT}test"
    done
    if [ -n "$COVERAGE" ]; then
	run_sbt scct-merge-report
    fi
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
