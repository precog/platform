package com.precog.ingest.service

import akka.dispatch.MessageDispatcher

import com.precog.ingest.api._
import com.precog.ingest.util.QuerioZookeeper

import java.util.Properties
import net.lag.configgy.ConfigMap
import scalaz.NonEmptyList

trait KafkaEventStoreComponent {

  implicit def defaultFutureDispatch: MessageDispatcher

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    val topicId = eventConfig.getString("topicId").getOrElse(sys.error("Invalid configuration eventStore.topicId required"))
    val zookeeperHosts = eventConfig.getString("zookeeperHosts").getOrElse(sys.error("Invalid configuration eventStore.zookeeperHosts required"))
    
    val props = new Properties()
    props.put("zk.connect", zookeeperHosts)
    props.put("serializer.class", "com.precog.ingest.api.IngestMessageCodec")
    
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)
    val messaging = new SimpleKafkaMessaging(topicId, props)

    val qz = QuerioZookeeper.testQuerioZookeeper(zookeeperHosts)
    qz.setup
    val producerId = qz.acquireProducerId
    qz.close

    new KafkaEventStore(new EventRouter(routeTable, messaging), producerId)
  }
}

object KafkaIngestServer extends IngestServer with KafkaEventStoreComponent with YggdrasilQueryExecutorComponent
