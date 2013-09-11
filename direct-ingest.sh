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

SRCDIR=muspelheim/src/test/resources/test_data
OWNERTOKEN=C18ED787-BF07-4097-B819-0415C759C8D5
OWNERACCOUNT=FakeAccount

function usage() {
    echo "Usage: `basename $0` [-n] [-t <owner token>] [-s <source directory>] <target data directory>" >&2
    echo "  -n : don't wipe existing data" >&2
    echo "  -t : Specify the owner token (defaults to $OWNERTOKEN)" >&2
    echo "  -o : Specify the owner account (defaults to $OWNERACCOUNT)" >&2
    echo "  -s : Specify the source directory (defaults to `dirname $0`/$SRCDIR)" >&2
    exit 1
}

VERSION=`git describe`

while getopts "nt:o:s:" OPTNAME; do
    case $OPTNAME in
        t)
            OWNERTOKEN=$OPTARG
            ;;
        o)
            OWNERACCOUNT=$OPTARG
            ;;
        s)
            [ -d $OPTARG ] || {
                echo "Could not open source directory: $OPTARG" >&2
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

if [[ $# != 1 ]]; then
    usage
fi

[ -d $1 ] || {
    echo "Could not open target directory: $1" >&2
    exit 2
}

DATADIR=$(cd $1; pwd)

cd `dirname $0`

echo "Loading data from $SRCDIR with owner token $OWNERTOKEN"

if [ ! -d $SRCDIR -o ! -d $DATADIR ]; then
    echo "Source or dest dir does not exist!" >&2
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

[ -f ratatoskr/target/ratatoskr-assembly-$VERSION.jar ] || {
    echo "Error: you must build ratatoskr/assembly before running tasks that extract data"
    exit 2
}

[ -z "$DONTWIPE" ] && rm -rf $DATADIR/*

echo "Importing to $DATADIR"

java -Xmx1G -cp ratatoskr/target/ratatoskr-assembly-$VERSION.jar com.precog.ratatoskr.Ratatoskr import -t $OWNERTOKEN -o $OWNERACCOUNT -s $DATADIR $SOURCES
