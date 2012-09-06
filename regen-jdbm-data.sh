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

if [[ $# != 1 ]]; then
    echo "Usage: `basename $0` <target data directory>"
    echo "  For now target is normally pandora/dist/data-jdbm/data/"
    exit
fi

SRCDIR=muspelheim/src/test/resources/test_data
DATADIR=$(cd $1; pwd)

cd `dirname $0`

if [ ! -d $SRCDIR -o ! -d $DATADIR ]; then
    echo "Source or dest dir does not exist!"
    exit 2
fi

pushd $SRCDIR > /dev/null
SOURCES=""
for source in `find . -name '*.json'`; do
    DPATH=`echo $source | sed -e 's/^\.\(.*\)\.json/\1\//'`
    DATAFILE=${source/\.\//}
    SOURCES="$SOURCES $DPATH=$SRCDIR/$DATAFILE"
done
popd > /dev/null

[ -f yggdrasil/target/yggdrasil-assembly-2.0.0-SNAPSHOT.jar ] || sbt yggdrasil/assembly

rm -rf $DATADIR/*

java -cp yggdrasil/target/yggdrasil-assembly-2.0.0-SNAPSHOT.jar com.precog.yggdrasil.util.YggUtils import -t C18ED787-BF07-4097-B819-0415C759C8D5 -s $DATADIR $SOURCES
