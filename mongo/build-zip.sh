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
cp -R quirrelide/build/* web/
rm -rf web/index.html web/php web/*.php
zip -ru $TMPDIR/precog.jar web
rm -rf web
cp precog.sh precog.bat config.cfg README.md CHANGELOG.md $TMPDIR/
zip -r precog.zip $TMPDIR


