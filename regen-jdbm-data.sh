#!/bin/bash

DATADIR=`dirname $0`/pandora/dist/data-jdbm/data

rm -rf $DATADIR/*

java -cp yggdrasil/target/yggdrasil-assembly-2.0.0-SNAPSHOT.jar com.precog.yggdrasil.util.YggUtils import -t C18ED787-BF07-4097-B819-0415C759C8D5 -s $DATADIR '/views=muspelheim/src/test/resources/test_data/views.json' '/clicks/=muspelheim/src/test/resources/test_data/clicks.json' '/campaigns/=muspelheim/src/test/resources/test_data/campaigns.json' '/obnoxious/=muspelheim/src/test/resources/test_data/obnoxious.json' '/organizations=muspelheim/src/test/resources/test_data/organizations.json' '/richie1/test=muspelheim/src/test/resources/test_data/richie1/test.json' '/test/empty_object=muspelheim/src/test/resources/test_data/test/empty_object.json' '/test/null=muspelheim/src/test/resources/test_data/test/null.json' '/test/empty_array=muspelheim/src/test/resources/test_data/test/empty_array.json' '/fastspring_nulls/=muspelheim/src/test/resources/test_data/fastspring_nulls.json' '/fastspring_mixed_type/=muspelheim/src/test/resources/test_data/fastspring_mixed_type.json'
