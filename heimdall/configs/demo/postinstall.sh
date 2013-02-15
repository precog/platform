#!/bin/bash

set -e

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
if ! monit unmonitor -g jobs-v1-demo; then
    echo "Monit unhappy on unmonitor of jobs-v1-demo"
fi


# Stop and start the service
if status jobs-v1-demo | grep running; then
    stop jobs-v1-demo
fi

sleep 5

if ! RESULT=`start jobs-v1-demo 2>&1` > /dev/null ; then
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

if ! monit monitor -g jobs-v1-demo; then
    echo "Monit unhappy on remonitor of jobs-v1-demo"
fi

# Wait 30 seconds for startup, then test the health URLs
sleep 30

echo "Running health checks"
curl -v -f -G "http://localhost:40066/blueeyes/services/jobs/v1/health"
echo "Completed health checks"

exit 0
