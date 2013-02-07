package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.client.BaseClient._
import com.precog.ingest.service._
import WebJobManager._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future }

import org.streum.configrity.Configuration

import scalaz._

object KafkaEventServer extends BlueEyesServer with EventService with AkkaDefaults {
  val clock = Clock.System
  implicit val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(defaultFutureDispatch)

  def APIKeyFinder(config: Configuration) = WebAPIKeyFinder(config)
  def AccountFinder(config: Configuration) = WebAccountFinder(config)
  def JobManager(config: Configuration) = WebJobManager(config).withM[Future]
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
