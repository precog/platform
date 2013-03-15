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

cd `dirname $0`

function usage {
    echo "Usage: $(basename $0) [-n]"
    echo "  -n: Skip proguard"
    exit 1
}

while getopts ":n" opt; do
    case $opt in
        n)
            SKIPPROGUARD=1
            ;;
        \?)
            usage
            ;;
    esac
done

if [[ `ls target/*.jar | wc -l` != 1 ]]; then
    echo "Missing/too many jars!"
    exit 1
fi

TMPDIR=precog

mkdir -p $TMPDIR
rm -rf $TMPDIR/*

if [ -z "$SKIPPROGUARD" ]; then
    java -Xmx2048m -jar ../tools/lib/proguard.jar @proguard.conf -injars target/mongo-assembly*.jar -outjars $TMPDIR/precog.jar | tee proguard.log
else
    cp target/mongo-assembly*.jar $TMPDIR/precog.jar
fi

mkdir web
cp -R quirrelide/build/* web/
rm -rf web/index.html web/php web/*.php
zip -ru $TMPDIR/precog.jar web
rm -rf web
cp precog.sh precog.bat config.cfg README.md CHANGELOG.md $TMPDIR/
zip -r precog.zip $TMPDIR


