#!/bin/bash

cd `dirname $0`

if [[ `ls target/*.jar | wc -l` != 1 ]]; then
    echo "Missing/too many jars!"
    exit 1
fi

TMPDIR=precog

mkdir -p $TMPDIR
rm -rf $TMPDIR/*

java -jar tools/proguard.jar @proguard.conf -injars target/mongo-assembly*.jar -outjars $TMPDIR/precog.jar | tee proguard.log 
mkdir web
zip -u $TMPDIR/precog.jar web
rmdir web
cp precog.sh precog.bat config.cfg $TMPDIR/
zip -r precog.zip $TMPDIR


