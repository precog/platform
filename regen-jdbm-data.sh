#!/bin/bash

function usage() {
    echo "Usage: `basename $0` [-t <owner token>] [-s <source directory>] <target data directory> <sbt launcher JAR>"
    echo "  For now target is normally pandora/dist/data-jdbm/data/"
    exit 1
}

SRCDIR=muspelheim/src/test/resources/test_data
OWNERTOKEN=C18ED787-BF07-4097-B819-0415C759C8D5

while getopts "t:s:" OPTNAME; do
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
for source in `find . -name '*.json'`; do
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

rm -rf $DATADIR/*

java -cp yggdrasil/target/yggdrasil-assembly-2.0.0-SNAPSHOT.jar com.precog.yggdrasil.util.YggUtils import -t $OWNERTOKEN -s $DATADIR $SOURCES
