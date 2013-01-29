#!/bin/bash

cd `dirname $0`

if [[ `ls target/*.jar | wc -l` != 1 ]]; then
    echo "Missing/too many jars!"
    exit 1
fi

TMPDIR=precog

mkdir -p $TMPDIR
rm -rf $TMPDIR/*

java -Xmx2048m -jar ../mongo/tools/proguard.jar @proguard.conf -injars target/jdbc-assembly*.jar -outjars $TMPDIR/precog.jar | tee proguard.log 
mkdir web
cp -R ../mongo/quirrelide/build/* web/
rm -rf web/index.html web/php web/*.php
zip -ru $TMPDIR/precog.jar web
rm -rf web
cp precog.sh precog.bat config.cfg README.md CHANGELOG.md $TMPDIR/
zip -r precog.zip $TMPDIR


