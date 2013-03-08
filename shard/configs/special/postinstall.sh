#!/bin/bash

set -e

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
if ! monit unmonitor -g shard-v2; then
    echo "Monit unhappy on unmonitor of shard-v2"
fi


# Stop and start the service
if status shard-v2 | grep running; then
    stop shard-v2
fi

sleep 5

if ! RESULT=`start shard-v2 2>&1` > /dev/null ; then
    if echo "$RESULT" | grep -v "already running" > /dev/null ; then
	echo "Failure: $RESULT"
	exit 1
    fi
    fi

# Restart monit to pick up changes
if ! monit reload; then
    echo "Monit unhappy on reload"
fi

sleep 5

if ! monit monitor -g shard-v2; then
    echo "Monit unhappy on remonitor of shard-v2"
fi

# Wait 30 seconds for startup, then test the health URLs
sleep 30

echo "Running health checks"
curl -v -f -G "http://localhost:30070/blueeyes/services/analytics/v2/health"
echo "Completed health checks"

exit 0
