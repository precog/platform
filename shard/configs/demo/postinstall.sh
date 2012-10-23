#!/bin/bash

set -e

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
for ID in 1 2 3 4; do 
    if ! monit unmonitor -g shard-v1-demo$ID; then
	echo "Monit unhappy on unmonitor of shard-v1-demo$ID"
    fi
done

# Stop and start the services
for ID in 1 2 3 4; do
    if status shard-v1-demo$ID | grep running; then
        stop shard-v1-demo$ID
    fi

    sleep 5

    if ! RESULT=`start shard-v1-demo$ID 2>&1` > /dev/null ; then
        if echo "$RESULT" | grep -v "already running" > /dev/null ; then
	    echo "Failure: $RESULT"
	    exit 1
        fi
    fi
done

# Restart monit to pick up changes
if ! monit reload; then
    echo "Monit unhappy on reload"
fi

sleep 5

for ID in 1 2 3 4; do 
    if ! monit monitor -g shard-v1-demo$ID; then
	echo "Monit unhappy on remonitor of shard-v1-demo$ID"
    fi
done

# Wait 30 seconds for startup, then test the health URLs
sleep 30

echo "Running health checks"
for PORT in 30150 30152 30154 30156; do
    curl -v -f -G "http://localhost:$PORT/blueeyes/services/quirrel/v1/health"
    echo ""
done
echo "Completed health checks"

exit 0
