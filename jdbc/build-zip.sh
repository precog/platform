#!/bin/bash

cd `dirname $0`

if [[ `ls target/*.jar | wc -l` != 1 ]]; then
    echo "Missing/too many jars!"
    exit 1
fi

while getopts ":n" opt; do
    case $opt in
        n)
            SKIPPG=1
            ;;
        \?)
            echo "Usage: $(basename $0) [-n]"
            echo "  -n: Skip proguard"
            exit 1
            ;;
    esac
done

TMPDIR=precog

mkdir -p $TMPDIR
rm -rf $TMPDIR/*

if [ -z "$SKIPPG" ]; then
    java -Xmx2048m -jar ../tools/lib/proguard.jar @proguard.conf -injars target/jdbc-assembly*.jar -outjars $TMPDIR/precog.jar | tee proguard.log
else
    cp target/jdbc-assembly*.jar $TMPDIR/precog.jar
fi

mkdir web
cp -R ../mongo/quirrelide/build/* web/
rm -rf web/index.html web/php web/*.php
zip -ru $TMPDIR/precog.jar web
rm -rf web
cp precog.sh precog.bat config.cfg README.md CHANGELOG.md $TMPDIR/
zip -r precog.zip $TMPDIR


