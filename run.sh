#!/bin/bash

MAX_SHARD_STARTUP_WAIT=300

function usage {
    echo "Usage: ./run.sh [-b] [-l] [-d] [-a <assemblies to build>] [-q directory] [ingest.json ...]" >&2
    echo "  -a: Force build of the specified assemblies" >&2
    echo "  -b: Build any required artifacts for the run" >&2
    echo "  -d: Print debug output" >&2
    echo "  -l: Don't clean work directory on completion" >&2
    echo "  -q: Read queries (any files not named *.pending) from the given query directory." >&2
    exit 1
}

if [ $# -eq 0 ]; then
    echo "Ingest files are required!"
    usage
fi

while getopts ":a:q:bld" opt; do
    case $opt in
        a)
            EXTRAFLAGS="$EXTRAFLAGS -a $OPTARG"
            ;;
        q)
            QUERYDIR=$OPTARG
            ;;
        b)
            EXTRAFLAGS="$EXTRAFLAGS -b"
            ;;
        l)
            DONTCLEAN=1
            ;;
        d)
            DEBUG=1
            ;;
        \?)
            echo "Unknown option $OPTARG!"
            usage
            ;;
    esac
done

shift $(( $OPTIND - 1 ))

WORKDIR=$(mktemp -d -t standaloneShard.XXXXXX 2>&1)
echo "Starting under $WORKDIR"
./start-shard.sh -d $WORKDIR $EXTRAFLAGS 1> $WORKDIR/shard.stdout &
RUN_LOCAL_PID=$!

# Wait to make sure things haven't died
sleep 2
if ! kill -0 $RUN_LOCAL_PID &> /dev/null ; then
    echo "Shard failed to start!"
    exit 2
else
    echo "Shard starting up..."
fi

function finished {
    echo "Hang on, killing start-shard.sh: $RUN_LOCAL_PID"
    kill $RUN_LOCAL_PID
    wait $RUN_LOCAL_PID
    if [ -z "$DONTCLEAN" -a "$EXIT_CODE" = "0" ]; then
        echo "Cleaning"
        rm -rf $WORKDIR
        rm -f results.json 2>/dev/null
    else
        echo "Skipping clean of $WORKDIR"
    fi
}

trap "finished; exit 1" TERM INT
trap "finished" EXIT

# Wait for the shard to come up fully
for trySeq in `seq 1 $MAX_SHARD_STARTUP_WAIT`; do
    if [ -f $WORKDIR/ports.txt ] > /dev/null; then
        break
    fi
    sleep 1
done

if [ ! -f $WORKDIR/ports.txt ] > /dev/null; then
    echo "Start must have failed to start! Leaving work dir in place!" >&2
    DONTCLEAN=1
    exit 1
fi

ROOTTOKEN="$(cat $WORKDIR/root_token.txt)"
ACCOUNTID="$(cat $WORKDIR/account_id.txt)"
TOKEN="$(cat $WORKDIR/account_token.txt)"

# start-shard.sh records the port assignments as sh-style vars in ports.txt
. $WORKDIR/ports.txt

echo "Work dir:      $WORKDIR"
echo "Root API key:  $ROOTTOKEN"
echo "Account ID:    $ACCOUNTID"
echo "Account token: $TOKEN"
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

function query {
    curl -s -G --data-urlencode "q=$1" --data-urlencode "apiKey=$TOKEN" "http://localhost:$SHARD_PORT/analytics/v2/analytics/fs/$ACCOUNTID"
}

function repl {
    while true; do
        read -p "quirrel> " QUERY
        if [ $? -ne 0 ]; then
            finished
            exit 0
        fi
        query "$QUERY"
        echo ""
    done
}

# Our current options for sync/streaming support, want to exercise all of them
SYNCFLAG[0]="&mode=streaming"
SYNCFLAG[1]="&mode=batch&receipt=true"
SYNCFLAG[2]="&mode=batch&receipt=false"

SYNCINDEX=0

for f in $@; do
    echo "Ingesting: $f"
    TABLE=$(basename "$f" ".json")
    ALLTABLES="$ALLTABLES $TABLE"
    COUNT=$(cat "$f" | wc -l)

    [ -n "$DEBUG" ] && echo -e "Posting curl -X POST --data-binary @\"$f\" \"http://localhost:$INGEST_PORT/ingest/v2/fs/$ACCOUNTID/$TABLE?apiKey=$TOKEN\""
    INGEST_RESULT=$(curl -s -S -v -X POST -H 'Content-Type: application/x-json-stream' --data-binary @"$f" "http://localhost:$INGEST_PORT/ingest/v2/fs/$ACCOUNTID/$TABLE?apiKey=$TOKEN${SYNCFLAG[$SYNCINDEX]}")

    SYNCINDEX=$(( ($SYNCINDEX + 1) % ${#SYNCFLAG[@]} ))

    [ -n "$DEBUG" ] && echo $INGEST_RESULT

    TRIES_LEFT=30

    COUNT_RESULT=$(query "count(//$TABLE)" | tr -d '[]')
    while [[ $TRIES_LEFT -gt 0 && ( -z "$COUNT_RESULT" || ${COUNT_RESULT:-0} -lt $COUNT ) ]] ; do
        [ -n "$DEBUG" ] && echo "Count result for $TABLE = ${COUNT_RESULT:-0} / $COUNT on try $TRIES_LEFT"
        sleep 2
        COUNT_RESULT=$(query "count(//$TABLE)" | tr -d '[]')
        TRIES_LEFT=$(( $TRIES_LEFT - 1 ))
    done

    [ "$TRIES_LEFT" -gt "0" ] || {
        echo "Exceeded maximum ingest count attempts for $TABLE. Expected $COUNT, got $COUNT_RESULT. Failure!"
	[ -n "$DEBUG" ] && sleep 86400 # Maybe excessive
        EXIT_CODE=1
        exit 1
    }

    [ -n "$DEBUG" ] && echo "Good count for $TABLE: $COUNT_RESULT"

    echo ""
done

EXIT_CODE=0

if [ "$QUERYDIR" = "" ]; then
    echo "TOKEN=$TOKEN"
    echo "WORKDIR=$WORKDIR"
    repl
else
    for f in $(find $QUERYDIR -type f ! -name '*.pending'); do
        query "$(cat $f)" > results.json
        RESULT="$(cat results.json)"

        if [ -n "$DEBUG" ]; then
            echo -e "Result for $f:"
            cat results.json
            echo ""
        fi

        if ! python -m json.tool results.json 1>/dev/null 2>/dev/null || [ "${RESULT:0:1}" != "[" ] || [ "${RESULT:0:2}" = "[]" ]; then
            echo "Query $f returned a bad result" 1>&2
            EXIT_CODE=1
        fi
    done

    [ "$EXIT_CODE" != "0" ] && echo "Queries failed!" || echo "Queries succeeded"

    # Test archive to make sure it works by actually removing all of our ingested data
    echo "Deleting ingested data"
    for TABLE in $ALLTABLES; do
        echo "  deleting $TABLE..."
        ARCHIVE_RESULT=$(curl -s -S -X DELETE "http://localhost:$INGEST_PORT/ingest/v2/fs/$ACCOUNTID/$TABLE?apiKey=$TOKEN")

        [ -n "$DEBUG" ] && echo $ARCHIVE_RESULT
    done

    # Give the shard some time to actually process the archives
    TRIES=18
    while [[ $TRIES -gt 0 ]]; do
        if [[ $(find $WORKDIR/shard-data/data -name projection_descriptor.json | wc -l) -gt 0 ]]  ; then
            [ -n "$DEBUG" ] && echo "Archived data still found, sleeping"
            [ -n "$DEBUG" ] && find $WORKDIR/shard-data/data -name projection_descriptor.json
            TRIES=$(( $TRIES - 1 ))
            sleep 10
        else
            break
        fi
    done

    if [[ $(find $WORKDIR/shard-data/data -name projection_descriptor.json | wc -l) -gt 0 ]]; then
        echo "Archive of datasets failed. Projections still found in data directory!" 1>&2
        EXIT_CODE=1
    else
        echo "Archive completed"
    fi

    # Test health check URLs for all services
    echo "Checking health URLs"

    echo -n "Checking ingest health at http://localhost:$INGEST_PORT/ingest/v2/health......."
    curl -sG "http://localhost:$INGEST_PORT/ingest/v2/health" > /dev/null && echo OK || {
        echo "Ingest health check failed" >&2
        EXIT_CODE=1
    }
    echo -n "Checking auth health at http://localhost:$AUTH_PORT/security/v1/health......."
    curl -sG "http://localhost:$AUTH_PORT/security/v1/health" > /dev/null && echo OK || {
        echo "Auth health check failed" >&2
        EXIT_CODE=1
    }
    echo -n "Checking accounts health at http://localhost:$ACCOUNTS_PORT/accounts/v1/health..."
    curl -sG "http://localhost:$ACCOUNTS_PORT/accounts/v1/health" > /dev/null && echo OK || {
        echo "Accounts health check failed" >&2
        EXIT_CODE=1
    }
    echo -n "Checking jobs health at http://localhost:$JOBS_PORT/jobs/v1/health..........."
    curl -sG "http://localhost:$JOBS_PORT/jobs/v1/health" > /dev/null && echo OK || {
        echo "Jobs health check failed" >&2
        EXIT_CODE=1
    }
    echo -n "Checking shard health at http://localhost:$SHARD_PORT/analytics/v2/health....."
    curl -sG "http://localhost:$SHARD_PORT/analytics/v2/health" > /dev/null && echo OK || {
        echo "Shard health check failed" >&2
        EXIT_CODE=1
    }

fi

exit $EXIT_CODE
