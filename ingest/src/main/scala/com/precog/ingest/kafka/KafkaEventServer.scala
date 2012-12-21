package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.ingest.service._

import blueeyes.bkka.{ AkkaDefaults, AkkaTypeClasses }
import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future, MessageDispatcher }

import java.util.Properties
import java.net.InetAddress

import com.weiglewilczek.slf4s.Logging
import org.streum.configrity.Configuration

import scalaz._

object KafkaEventServer extends 
    BlueEyesServer with 
    EventService with 
    WebAccountFinderComponent with
    WebAPIKeyFinderComponent with
    KafkaEventStoreComponent {

  val clock = Clock.System

  implicit val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
}


trait KafkaEventStoreComponent extends WebAccountFinderComponent with AkkaDefaults with Logging {
  def EventStore(config: Configuration): EventStore = {
    val centralZookeeperHosts = getConfig(config, "central.zk.connect")

    val serviceUID = ZookeeperSystemCoordination.extractServiceUID(config)
    val coordination = ZookeeperSystemCoordination(centralZookeeperHosts, serviceUID, true)
    val agent = serviceUID.hostId + serviceUID.serviceId  

    val localConfig = config.detach("local")

    val eventIdSeq = new SystemEventIdSequence(agent, coordination)
    val accountFinder = AccountFinder(config.detach("accountFinder"))
    val eventStore = new LocalKafkaEventStore(localConfig)
    val relayAgent = new KafkaRelayAgent(accountFinder, eventIdSeq, localConfig, config.detach("central"))

    new EventStore {
      def save(action: Event, timeout: Timeout) = eventStore.save(action, timeout)
      def start() = relayAgent.start flatMap { _ => eventStore.start }
      def stop() = eventStore.stop flatMap { _ => relayAgent.stop }
    }
  }

  def getConfig(cfg: Configuration, key: String): String = cfg.get[String](key) getOrElse {
    sys.error("Invalid configuration eventStore.%s required".format(key))
  }
}

