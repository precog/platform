package com.precog.ingest
package kafka

import akka.dispatch.MessageDispatcher

import com.precog.common._
import com.precog.common.util.ZookeeperSystemCoordination

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import java.net.InetAddress

import net.lag.configgy.ConfigMap
import scalaz.NonEmptyList

trait KafkaEventStoreComponent {

  implicit def defaultFutureDispatch: MessageDispatcher

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    val localTopic = getConfig(eventConfig, "local.topic")
    val localBroker = getConfig(eventConfig, "local.broker")
    
    val centralTopic = getConfig(eventConfig, "central.topic")
    val centralZookeeperHosts = getConfig(eventConfig, "central.zookeeperHosts")

    val localConfig = new Properties()
    localConfig.put("broker.list", localBroker)
    localConfig.put("serializer.class", "com.precog.ingest.kafka.KafkaEventCodec")
    
    val centralConfig = new Properties()
    centralConfig.put("zk.connect", centralZookeeperHosts)
    centralConfig.put("serializer.class", "com.precog.ingest.kafka.KafkaIngestMessageCodec")

    val coordination = ZookeeperSystemCoordination.testZookeeperSystemCoordination(centralZookeeperHosts)

    val agent = InetAddress.getLocalHost.getHostName

    val eventIdSeq = new SystemEventIdSequence(agent, coordination)

    val eventStore = new LocalKafkaEventStore(localTopic, localConfig)
    val relayAgent = new KafkaRelayAgent(eventIdSeq, localTopic, localConfig, centralTopic, centralConfig)

    new EventStore {
      def save(event: Event) = eventStore.save(event)
      def start() = relayAgent.start flatMap { _ => eventStore.start }
      def stop() = eventStore.stop flatMap { _ => relayAgent.stop }
    }
  }

  def getConfig(cfg: ConfigMap, key: String): String = cfg.getString(key).getOrElse(
    sys.error("Invalid configuration eventStore.%s required".format(key))
  )

}

object KafkaIngestServer extends IngestServer with KafkaEventStoreComponent
