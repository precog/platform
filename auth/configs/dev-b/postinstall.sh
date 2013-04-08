#!/bin/bash

set -e

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
if ! monit unmonitor -g auth-v1-b; then
    echo "Monit unhappy on unmonitor of auth-v1-b"
fi


# Stop and start the service
if status auth-v1-b | grep running; then
    stop auth-v1-b
fi

sleep 5

if ! RESULT=`start auth-v1-b 2>&1` > /dev/null ; then
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

if ! monit monitor -g auth-v1-b; then
    echo "Monit unhappy on remonitor of auth-v1-b"
fi

# Wait 30 seconds for startup, then test the health URLs
sleep 30

echo "Running health checks"
curl -v -f -G "http://localhost:31062/security/v1/health"
echo "Completed health checks"

exit 0
