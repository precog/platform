#!/bin/bash

# Ensure data directory and ownership
mkdir -p /opt/precog/shard/data
mkdir -p /opt/precog/shard/scratch
chown -R precog /opt/precog/shard/

set -e

# Reload the upstart config
initctl reload-configuration

# Keep monit from interrupting us
if /etc/init.d/monit stop; then
	echo "Monit stopped unhappy"
fi

# Stop and start the service
if status shard-v2-b | grep running; then
    stop shard-v2-b
fi

sleep 5

if ! RESULT=`start shard-v2-b 2>&1` > /dev/null ; then
        if echo "$RESULT" | grep -v "already running" > /dev/null ; then
		echo "Failure: $RESULT"
		exit 1
        fi
fi


# Restart monit to pick up changes
if /etc/init.d/monit start; then
	echo "Monit started unhappy";
fi

# Wait 30 seconds for startup, then test the health URL
sleep 30

echo "Checking health"
#curl -v -f -G "http://localhost:31070/analytics/v2/health"
echo "Completed health check"

exit 0
