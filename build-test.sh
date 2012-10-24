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

cd "$(dirname $0)"

# stolen shamelessly from start-shard.sh
port_is_open() {
	netstat -an | egrep "[\.:]$1[[:space:]]+.*LISTEN" > /dev/null
}

wait_until_port_open() {
    while ! port_is_open $1; do
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
