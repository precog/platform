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
