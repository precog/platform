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

cd quirrel && ln -fs ../../research/quirrel/examples
cd ..

SUCCESS=0

sbt -mem 2048 -J-Dsbt.log.noformat=true clean
SUCCESS=$(($SUCCESS || $?))

sbt -mem 2048 -J-Dsbt.log.noformat=true compile
SUCCESS=$(($SUCCESS || $?))

sbt -mem 2048 -J-Dsbt.log.noformat=true test:compile
SUCCESS=$(($SUCCESS || $?))

sbt -mem 2048 -J-Dsbt.log.noformat=true yggdrasil/assembly
SUCCESS=$(($SUCCESS || $?))

# For the runs, we don't want to terminate early if a particular project fails
set +e
for PROJECT in util common daze auth accounts ragnarok ingest bytecode quirrel muspelheim yggdrasil shard pandora; do
  sbt -mem 2048 -J-Dsbt.log.noformat=true "$PROJECT/test"
  SUCCESS=$(($SUCCESS || $?))
done

if [ $SUCCESS -eq 0 ]; then
  sbt -mem 2048 accounts/assembly auth/assembly ingest/assembly yggdrasil/assembly shard/assembly
  SUCCESS=$(($SUCCESS || $?))
fi

wait_until_port_open 27117

shard/test.sh -m 27117
SUCCESS=$(($SUCCESS || $?))

# re-enable errexit
set -e

# For the triggers
if [ $SUCCESS -eq 0 ]; then
  echo "Finished: SUCCESS"
else
  echo "Finished: FAILURE"
fi

exit $SUCCESS
