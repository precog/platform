#!/bin/bash

set -e

VERSION=`git describe`
pushd ..
	sbt desktop/assembly
popd

TMPDIR=precog
mkdir -p $TMPDIR
rm -rf $TMPDIR/*
java -Xmx2048m -jar ../tools/lib/proguard.jar @proguard.conf -injars target/desktop-assembly-$VERSION.jar -outjars $TMPDIR/precog-desktop.jar 2>&1 | tee proguard.log 
test -d web || mkdir web
cp -R ../../quirrelide/build/* web/
rm -rf web/php web/*.php
zip -ru $TMPDIR/precog-desktop.jar web
rm -rf web
