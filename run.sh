#!/bin/bash

function usage {
    echo "Usage: ./run.sh [-q directory] [ingest.json ...]" 1>&2
    exit 1
}

if [ $# -eq 0 ]; then
    echo "Ingest files are required!"
    usage
fi

while getopts ":q:" opt; do
    case $opt in
        q)
            QUERYDIR=$OPTARG
            shift
            shift
            ;;
        \?)
            echo "Unknown option $OPTARG!"
            usage
            ;;
    esac
done

INGEST_PORT=30060
QUERY_PORT=30070

WORKDIR=$(mktemp -d -t standaloneShard.XXXXXX 2>&1)
echo "Starting..."
./start-shard.sh -d $WORKDIR 2>/dev/null 1>/dev/null &
RUN_LOCAL_PID=$!

function finished {
    echo "Hang on, killing start-shard.sh: $RUN_LOCAL_PID"
    kill $RUN_LOCAL_PID
    wait $RUN_LOCAL_PID
    echo "Cleaning"
    rm -rf $WORKDIR
}

trap "finished; exit 1" TERM INT

while ! netstat -an | grep $INGEST_PORT > /dev/null; do
    sleep 1
done

TOKEN=$(cat $WORKDIR/root_token.txt)

for f in $@; do
    echo "Ingesting: $f"
    ./muspelheim/src/test/python/newlinejson.py $f | curl -X POST --data-binary @- "http://localhost:$INGEST_PORT/sync/fs/$(basename "$f" ".json")?apiKey=$TOKEN"
    echo ""
done

function query {
    curl -s -m 60 -G --data-urlencode "q=$1" --data-urlencode "apiKey=$TOKEN" "http://localhost:$QUERY_PORT/analytics/fs/"
}

function repl {
    while true; do
        read -p "quirrel> " QUERY
        if [ $? -ne 0 ]; then
            finished
            exit 0
        fi
        query $QUERY
        echo ""
    done
}

EXIT_CODE=0

if [ "$QUERYDIR" = "" ]; then
    echo "TOKEN=$TOKEN"
    echo "WORKDIR=$WORKDIR"
    repl
else
    for f in $(find $QUERYDIR -type f); do
        RESULT=$(query "$(cat $f)")

        echo $RESULT | python -m json.tool 1>/dev/null 2>/dev/null
        VALID_JSON=$?

        if [ ! $VALID_JSON ] || [ ${RESULT:0:1} != "[" ] || [ $RESULT = "[]" ]; then
            echo "Query $f returned a bad result" 1>&2
            EXIT_CODE=1
        fi
    done
fi

finished
exit $EXIT_CODE
