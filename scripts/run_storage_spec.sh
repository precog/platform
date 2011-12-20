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

cd `dirname $0`/..

WORKDIR=/tmp/foo
CLASSPATH=storage/target/leveldb-assembly-0.0.1-SNAPSHOT.jar:storage/target/scala-2.9.1/test-classes

rm -rf $WORKDIR
mkdir $WORKDIR
java -cp $CLASSPATH com.reportgrid.storage.leveldb.ReadTest $WORKDIR 1000 42

for spec in "$@"; do
  java -cp $CLASSPATH:/home/software/ivyrepo/cache/org.specs2/specs2-scalaz-core_2.9.1/jars/specs2-scalaz-core_2.9.1-6.0.1.jar:/home/software/ivyrepo/cache/org.specs2/specs2_2.9.1/jars/specs2_2.9.1-1.7-SNAPSHOT.jar specs2.run com.reportgrid.${spec}
done
