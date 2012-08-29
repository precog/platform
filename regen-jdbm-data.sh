#!/bin/bash

if [[ $# != 1 ]]; then
    echo "Usage: `basename $0` <target data directory>"
    echo "  For now target is normally pandora/dist/data-jdbm/data/"
    exit
fi

SRCDIR=muspelheim/src/test/resources/test_data
DATADIR=$1

cd `dirname $0`

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
