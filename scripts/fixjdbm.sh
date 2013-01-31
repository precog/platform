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
