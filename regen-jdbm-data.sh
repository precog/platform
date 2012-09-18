#!/bin/bash

if [[ $# < 2 || $# > 3 ]]; then
    echo "Usage: `basename $0` <target data directory> <sbt launcher JAR> [<owner token>]"
    echo "  For now target is normally pandora/dist/data-jdbm/data/"
    exit
fi

SRCDIR=muspelheim/src/test/resources/test_data
DATADIR=$(cd $1; pwd)

cd `dirname $0`

OWNERTOKEN=${3:-C18ED787-BF07-4097-B819-0415C759C8D5}

echo "Using token $OWNERTOKEN for data"

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
