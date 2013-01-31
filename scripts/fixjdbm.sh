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

[ "$#" -eq 1 ] || {
    echo "Usage: fixjdbm.sh <service name>"
    exit 1
}

[ -f "byIdentity.d.0" ] || {
    echo "This script needs to be run from a jdbm/ directory for a projection!"
    exit 1
}

mkdir recovery
monit unmonitor $1
stop $1
cp byIdentity* recovery/
cd recovery
~ubuntu/scala-2.9.2/bin/scala -cp /usr/share/java/$1.jar ~ubuntu/fixjdbm.scala
cp fixed/* ../
start $1
monit monitor $1
