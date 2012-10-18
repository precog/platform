#!/bin/bash

function usage() {
    echo "Usage: `basename $0` [-n] [-t <owner token>] [-s <source directory>] <target data directory>"
    echo "  For now target is normally pandora/dist/data-jdbm/data/"
    echo "  -n : don't wipe existing data"
    exit 1
}

VERSION=`sed -n 's/.*version.*:=.*"\(.*\)".*/\1/p' project/Build.scala`
SRCDIR=muspelheim/src/test/resources/test_data
OWNERTOKEN=C18ED787-BF07-4097-B819-0415C759C8D5

while getopts "nt:s:" OPTNAME; do
    case $OPTNAME in
        t)
            OWNERTOKEN=$OPTARG
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

[ -f yggdrasil/target/yggdrasil-assembly-$VERSION.jar ] || {
    echo "Error: you must build yggdrasil/assembly before running tasks that extract data"
    exit 2
}

[ -z "$DONTWIPE" ] && rm -rf $DATADIR/*

java -Xmx1G -cp yggdrasil/target/yggdrasil-assembly-$VERSION.jar com.precog.yggdrasil.util.YggUtils import -t $OWNERTOKEN -s $DATADIR $SOURCES
