package com.precog.ingest
package kafka

import blueeyes.bkka.AkkaDefaults
import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import akka.util.Timeout
import akka.dispatch.MessageDispatcher

import com.precog.common._
import com.precog.common.security._
import com.precog.ingest.service._
import com.precog.common.util.ZookeeperSystemCoordination
import com.precog.common.security._

import java.util.Properties

import java.net.InetAddress

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

object KafkaIngestServer extends 
    BlueEyesServer with 
    IngestService with 
    MongoTokenManagerComponent with
    KafkaEventStoreComponent {

  val clock = Clock.System

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")
}



trait KafkaEventStoreComponent extends AkkaDefaults with Logging {

  def eventStoreFactory(config: Configuration): EventStore = {

    val centralZookeeperHosts = getConfig(config, "central.zk.connect")

    val serviceUID = ZookeeperSystemCoordination.extractServiceUID(config)
    val coordination = ZookeeperSystemCoordination(centralZookeeperHosts, serviceUID)
    val agent = serviceUID.hostId + serviceUID.serviceId  

    val eventIdSeq = new SystemEventIdSequence(agent, coordination)

    val localConfig = config.detach("local")
    val centralConfig = config.detach("central")

    val eventStore = new LocalKafkaEventStore(localConfig)
    val relayAgent = new KafkaRelayAgent(eventIdSeq, localConfig, centralConfig)

    new EventStore {
      def save(event: Event, timeout: Timeout) = eventStore.save(event, timeout)
      def start() = relayAgent.start flatMap { _ => eventStore.start }
      def stop() = eventStore.stop flatMap { _ => relayAgent.stop }
    }
  }

  def getConfig(cfg: Configuration, key: String): String = cfg.get[String](key).getOrElse(
    sys.error("Invalid configuration eventStore.%s required".format(key))
  )

}

