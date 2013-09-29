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
