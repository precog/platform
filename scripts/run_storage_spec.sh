#!/bin/bash

cd `dirname $0`/../storage

CLASSPATH=target/leveldb-assembly-0.0.1-SNAPSHOT.jar:target/scala-2.9.1/test-classes:$HOME/.ivy2/cache/org.specs2/specs2-scalaz-core_2.9.1/jars/specs2-scalaz-core_2.9.1-6.0.1.jar:$HOME/.ivy2/cache/org.specs2/specs2_2.9.1/jars/specs2_2.9.1-1.7-SNAPSHOT.jar

for spec in leveldb.ColumnSpec ; do
  java -cp $CLASSPATH specs2.run com.precog.storage.${spec}
done
