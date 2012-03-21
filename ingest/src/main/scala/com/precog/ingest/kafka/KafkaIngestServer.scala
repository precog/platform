package com.precog.ingest
package kafka

import blueeyes.bkka.AkkaDefaults
import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import akka.util.Timeout
import akka.dispatch.MessageDispatcher

import com.precog.common._
import com.precog.ingest.service._
import com.precog.common.util.ZookeeperSystemCoordination
import com.precog.common.security.StaticTokenManager

import java.util.Properties

import java.net.InetAddress

import org.streum.configrity.Configuration

object KafkaIngestServer extends BlueEyesServer with IngestService with KafkaEventStoreComponent {

  val clock = Clock.System

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")

  def tokenManagerFactory(config: Configuration) = new StaticTokenManager

}

trait KafkaEventStoreComponent extends AkkaDefaults {

  def eventStoreFactory(config: Configuration): EventStore = {

    val centralZookeeperHosts = getConfig(config, "central.zk.connect")

    val coordination = ZookeeperSystemCoordination.testZookeeperSystemCoordination(centralZookeeperHosts)

    val agent = InetAddress.getLocalHost.getHostName + System.getProperty("precog.shard.suffix", "")

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

