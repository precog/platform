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

DATADIR=`dirname $0`/pandora/dist/data-jdbm/data

rm -rf $DATADIR/*

java -cp yggdrasil/target/yggdrasil-assembly-2.0.0-SNAPSHOT.jar com.precog.yggdrasil.util.YggUtils import -t C18ED787-BF07-4097-B819-0415C759C8D5 -s $DATADIR '/views=muspelheim/src/test/resources/test_data/views.json' '/clicks/=muspelheim/src/test/resources/test_data/clicks.json' '/campaigns/=muspelheim/src/test/resources/test_data/campaigns.json' '/obnoxious/=muspelheim/src/test/resources/test_data/obnoxious.json' '/organizations=muspelheim/src/test/resources/test_data/organizations.json' '/richie1/test=muspelheim/src/test/resources/test_data/richie1/test.json' '/test/empty_object=muspelheim/src/test/resources/test_data/test/empty_object.json' '/test/null=muspelheim/src/test/resources/test_data/test/null.json' '/test/empty_array=muspelheim/src/test/resources/test_data/test/empty_array.json' '/fastspring_nulls/=muspelheim/src/test/resources/test_data/fastspring_nulls.json' '/fastspring_mixed_type/=muspelheim/src/test/resources/test_data/fastspring_mixed_type.json'
