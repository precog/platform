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

function usage {
    echo "Usage: ./run.sh [-b] [-l] [-d] [-m <mongo port>] [-q directory] [ingest.json ...]" >&2
    echo "  -b: Build any required artifacts for the run" >&2
    echo "  -l: Don't clean work directory on completion" >&2
    echo "  -d: Print debug output" >&2
    exit 1
}

if [ $# -eq 0 ]; then
    echo "Ingest files are required!"
    usage
fi

while getopts ":q:m:bld" opt; do
    case $opt in
        q)
            QUERYDIR=$OPTARG
            ;;
        m)
            echo "Overriding mongo port to $OPTARG"
            MONGOPORT="-m $OPTARG"
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

INGEST_PORT=30060
QUERY_PORT=30070

WORKDIR=$(mktemp -d -t standaloneShard.XXXXXX 2>&1)
echo "Starting under $WORKDIR"
./start-shard.sh -d $WORKDIR $EXTRAFLAGS $MONGOPORT 1> $WORKDIR/shard.stdout &
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
    if [ -z "$DONTCLEAN" ]; then
        echo "Cleaning"
        rm -rf $WORKDIR
        rm results.json 2>/dev/null
    fi
}

trap "finished; exit 1" TERM INT

while ! netstat -an | grep $INGEST_PORT > /dev/null; do
    sleep 1
done

ROOTTOKEN="$(cat $WORKDIR/root_token.txt)"
ACCOUNTID="$(cat $WORKDIR/account_id.txt)"
TOKEN="$(cat $WORKDIR/account_token.txt)"
echo "Work dir:      $WORKDIR"
echo "Root API key:  $ROOTTOKEN"
echo "Account ID:    $ACCOUNTID"
echo "Account token: $TOKEN"

function query {
    curl -s -G --data-urlencode "q=$1" --data-urlencode "apiKey=$TOKEN" "http://localhost:$QUERY_PORT/analytics/fs/$ACCOUNTID"
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

for f in $@; do
    echo "Ingesting: $f"
    TABLE=$(basename "$f" ".json")
    DATA=$(./muspelheim/src/test/python/newlinejson.py $f)
    COUNT=$(echo "$DATA" | wc -l)
    echo -e "Posting curl -X POST --data-binary @- \"http://localhost:$INGEST_PORT/sync/fs/$ACCOUNTID/$TABLE?apiKey=$TOKEN\""
    INGEST_RESULT=$(echo "$DATA" | curl -s -S -X POST --data-binary @- "http://localhost:$INGEST_PORT/sync/fs/$ACCOUNTID/$TABLE?apiKey=$TOKEN")

    [ -n "$DEBUG" ] && echo $INGEST_RESULT

    COUNT_RESULT=$(query "count(//$TABLE)" | tr -d '[]')
    while [ -z "$COUNT_RESULT" ] || [ "$COUNT_RESULT" -lt "$COUNT" ]; do
        [ -n "$DEBUG" ] && echo "Count result for $TABLE = $COUNT_RESULT"
        sleep 2
        COUNT_RESULT=$(query "count(//$TABLE)" | tr -d '[]')
    done

    [ -n "$DEBUG" ] && echo "Good count for $TABLE"

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
fi

finished
exit $EXIT_CODE
