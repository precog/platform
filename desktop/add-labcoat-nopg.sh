#!/bin/bash

cd `dirname $0`

if [[ `ls target/*.jar | wc -l` != 1 ]]; then
    echo "Missing/too many jars!"
    exit 1
fi

TMPDIR=precog

mkdir -p $TMPDIR
rm -rf $TMPDIR/*

cp target/desktop-assembly*.jar $TMPDIR/precog-desktop.jar
mkdir web
cp -R ../../quirrelide/build/* web/
rm -rf web/php web/*.php
zip -ru $TMPDIR/precog-desktop.jar web
rm -rf web

