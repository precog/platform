package com.precog.shard

import com.precog.ingest._
import com.precog.ingest.kafka._
import com.precog.ingest.yggdrasil._

object KafkaShardServer extends IngestServer with KafkaEventStoreComponent with YggdrasilQueryExecutorComponent 
