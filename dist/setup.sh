#!/bin/bash

cd $(dirname $0)

. common.sh

if [ ! -x $MONGO_DIR/mongo ]; then
    echo "mongo command was not found under $MONGO_DIR - please update config."
    exit 1
fi

mkdir -p $ZKBASE $KFBASE $ZKDATA "$WORKDIR"/{configs,configs/templates,logs,shard-data/data,shard-data/archive,shard-data/scratch,shard-data/ingest_failures}

# Get a root token
if [ ! -e "$WORKDIR"/root_token.txt ]; then
    echo "Retrieving new root token"
    $JAVA $REBEL_OPTS -jar "$RATATOSKR_ASSEMBLY" tokens -s "localhost:$MONGO_PORT" -d dev_auth_v1 -c | tail -n 1 > "$WORKDIR"/root_token.txt || {
        echo "Error retrieving new root token" >&2
        exit 3
    }
fi

TOKENID=$(cat "$WORKDIR"/root_token.txt)


# Make root user have root token
echo -e "db.accounts.update({\"accountId\":\"$ROOT_ACCOUNT_ID\"},{\$set:{\"apiKey\":\"$TOKENID\"}})" | $MONGO_DIR/mongo --port $MONGO_PORT dev_accounts_v1


# Zookeeper
pushd $ZKBASE > /dev/null
tar --strip-components=1 --exclude='docs*' --exclude='src*' --exclude='dist-maven*' --exclude='contrib*' --exclude='recipes*' -xvzf "$ARTIFACTDIR"/zookeeper* > /dev/null 2>&1 || {
    echo "Failed to unpack zookeeper" >&2
    exit 3
}
popd > /dev/null

echo "# the directory where the snapshot is stored." >> $ZKBASE/conf/zoo.cfg
echo "dataDir=$ZKDATA" >> $ZKBASE/conf/zoo.cfg
echo "# the port at which the clients will connect" >> $ZKBASE/conf/zoo.cfg
echo "clientPort=$ZOOKEEPER_PORT" >> $ZKBASE/conf/zoo.cfg

# Zookeeper logging
cat > $ZKBASE/bin/log4j.properties <<EOF
log4j.rootLogger=INFO, file
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=$WORKDIR/logs/zookeeper.log
log4j.appender.file.MaxFileSize=1MB
log4j.appender.file.MaxBackupIndex=1
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{ABSOLUTE} %5p %c{1}:%L - %m%n
EOF


# Kafka
cd "$WORKDIR"
unzip -u "$ARTIFACTDIR"/kafka* > /dev/null || {
    echo "Failed to unpack kafka" >&2
    exit 3
}

cd "$WORKDIR"/kafka/config
chmod +x $KFBASE/bin/kafka-server-start.sh

sed -e "s#log.dir=.*#log.dir=$KFGLOBALDATA#; s/port=.*/port=$KAFKA_GLOBAL_PORT/; s/zk.connect=localhost:2181/zk.connect=localhost:$ZOOKEEPER_PORT/" < server.properties > server-global.properties
sed -e "s#log.dir=.*#log.dir=$KFLOCALDATA#; s/port=.*/port=$KAFKA_LOCAL_PORT/; s/enable.zookeeper=.*/enable.zookeeper=false/; s/zk.connect=localhost:2181/zk.connect=localhost:$ZOOKEEPER_PORT/" < server.properties > server-local.properties


# Ingest and Shard
sed -e "s#port = 30062#port = $AUTH_PORT#; \
	s#rootKey = .*#rootKey = \"$TOKENID\"#; \
	s#/var/log#$WORKDIR/logs#; \
	s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#" < \
	"$BASEDIR"/templates/dev-auth-v1.conf > \
	"$WORKDIR"/configs/auth-v1.conf || echo "Failed to update auth config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < \
	"$BASEDIR"/templates/dev-auth-v1.logging.xml > \
	"$WORKDIR"/configs/auth-v1.logging.xml

sed -e "s#port = 30064#port = $ACCOUNTS_PORT#; \
	s#/var/log#$WORKDIR/logs#; \
	s#port = 30062#port = $AUTH_PORT#; \
	s#rootKey = .*#rootKey = \"$TOKENID\"#; \
	s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#; \
        s#/etc/precog/templates#$WORKDIR/configs/templates#; \
	s#hosts = localhost:2181#hosts = localhost:$ZOOKEEPER_PORT#" < \
	"$BASEDIR"/templates/accounts-v1.conf > \
	"$WORKDIR"/configs/accounts-v1.conf || echo "Failed to update accounts config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < \
	"$BASEDIR"/templates/accounts-v1.logging.xml > \
	"$WORKDIR"/configs/accounts-v1.logging.xml

# Accounts email templates
cp "$BASEDIR"/templates/reset.* "$WORKDIR"/configs/templates/

sed -e "s#port = 30066#port = $JOBS_PORT#; \
	s#/var/log#$WORKDIR/logs#; \
	s#port = 30062#port = $AUTH_PORT#; \
	s#rootKey = .*#rootKey = \"$TOKENID\"#; \
	s#\[\"localhost\"\]#\[\"localhost:$MONGO_PORT\"\]#; \
	s#hosts = localhost:2181#hosts = localhost:$ZOOKEEPER_PORT#" < \
	"$BASEDIR"/templates/jobs-v1.conf > \
	"$WORKDIR"/configs/jobs-v1.conf || echo "Failed to update jobs config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < \
	"$BASEDIR"/templates/jobs-v1.logging.xml > \
	"$WORKDIR"/configs/jobs-v1.logging.xml

sed -e "s/port = 30060/port = $INGEST_PORT/; \
	s#/var/log#$WORKDIR/logs#; \
	s#port = 30062#port = $AUTH_PORT#; \
	s#rootKey = .*#rootKey = \"$TOKENID\"#;
	s#port = 30064#port = $ACCOUNTS_PORT#; \
	s#port = 30066#port = $JOBS_PORT#; \
	s#port = 30070#port = $SHARD_PORT#; \
	s#port = 9082#port = $KAFKA_LOCAL_PORT#; \
	s#port = 9092#port = $KAFKA_GLOBAL_PORT#; \
	s#connect = localhost:2181#connect = localhost:$ZOOKEEPER_PORT#" < \
	"$BASEDIR"/templates/ingest-v2.conf > \
	"$WORKDIR"/configs/ingest-v2.conf || echo "Failed to update ingest config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < "$BASEDIR"/templates/ingest-v2.logging.xml > "$WORKDIR"/configs/ingest-v2.logging.xml

sed -e "s#port = 30070#port = $SHARD_PORT#; \
	s#/var/log#$WORKDIR/logs#; \
	s#/opt/precog/shard#$WORKDIR/shard-data#; \
	s#port = 30062#port = $AUTH_PORT#; \
	s#rootKey = .*#rootKey = \"$TOKENID\"#; \
	s#port = 30064#port = $ACCOUNTS_PORT#; \
	s#port = 30066#port = $JOBS_PORT#; \
	s#port = 9092#port = $KAFKA_GLOBAL_PORT#; \
	s#hosts = localhost:2181#hosts = localhost:$ZOOKEEPER_PORT#" < \
	"$BASEDIR"/templates/shard-v2.conf > \
	"$WORKDIR"/configs/shard-v2.conf || echo "Failed to update shard config"
sed -e "s#/var/log/precog#$WORKDIR/logs#" < \
	"$BASEDIR"/templates/shard-v2.logging.xml > \
	"$WORKDIR"/configs/shard-v2.logging.xml

cd "$BASEDIR"


# Prior to ingest startup, we need to set an initial checkpoint if it's not already there
if [ ! -e "$WORKDIR"/initial_checkpoint.json ]; then
    $JAVA $REBEL_OPTS -jar "$RATATOSKR_ASSEMBLY" zk -z "localhost:$ZOOKEEPER_PORT" -uc "/precog-dev/shard/checkpoint/`hostname`:initial" &> $WORKDIR/logs/checkpoint_init.stdout || {
        echo "Couldn't set initial checkpoint!" >&2
        exit 3
    }
    touch "$WORKDIR"/initial_checkpoint.json
fi


echo "Finished setup"
echo "============================================================"
echo "Root token: $TOKENID"
echo "============================================================"
