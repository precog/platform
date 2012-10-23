#!/bin/sh
cd $(dirname $0)/..
exec ./run.sh -q shard/src/test/resources/queries/ muspelheim/src/test/resources/test_data/*.json
