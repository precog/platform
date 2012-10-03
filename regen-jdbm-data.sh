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

function usage() {
    echo "Usage: `basename $0` [-n] [-t <owner token>] [-s <source directory>] <target data directory> <sbt launcher JAR>"
    echo "  For now target is normally pandora/dist/data-jdbm/data/"
    echo "  -n : don't wipe existing data"
    exit 1
}

SRCDIR=muspelheim/src/test/resources/test_data
OWNERTOKEN=C18ED787-BF07-4097-B819-0415C759C8D5

while getopts "nt:s:" OPTNAME; do
    case $OPTNAME in
        t)
            OWNERTOKEN=$OPTARG
            ;;
        s)
            [ -d $OPTARG ] || {
                echo "Could not open source directory: $OPTARG"
                exit 2
            }
            SRCDIR=$OPTARG
            ;;
        n)
            echo "Not wiping existing data"
            DONTWIPE=true
            ;;
        \?)
            usage
            ;;
    esac
done

shift $(( $OPTIND - 1 ))

if [[ $# != 2 ]]; then
    usage
fi

[ -d $1 ] || {
    echo "Could not open target directory: $1"
    exit 2
}

DATADIR=$(cd $1; pwd)

cd `dirname $0`

echo "Loading data from $SRCDIR with owner token $OWNERTOKEN"

if [ ! -d $SRCDIR -o ! -d $DATADIR ]; then
    echo "Source or dest dir does not exist!"
    exit 2
fi

pushd $SRCDIR > /dev/null
SOURCES=""
for source in `find -L . -name '*.json'`; do
    DPATH=`echo $source | sed -e 's/^\.\(.*\)\.json/\1\//'`
    DATAFILE=${source/\.\//}
    SOURCES="$SOURCES $DPATH=$SRCDIR/$DATAFILE"
done
popd > /dev/null

[ -f yggdrasil/target/yggdrasil-assembly-2.0.0-SNAPSHOT.jar ] || {
    for target in "yggdrasil/compile" "yggdrasil/assembly"; do
        java -jar $2 "$target"
    done
}

[ -z "$DONTWIPE" ] && rm -rf $DATADIR/*

java -Xmx1G -cp yggdrasil/target/yggdrasil-assembly-2.0.0-SNAPSHOT.jar com.precog.yggdrasil.util.YggUtils import -t $OWNERTOKEN -s $DATADIR $SOURCES
