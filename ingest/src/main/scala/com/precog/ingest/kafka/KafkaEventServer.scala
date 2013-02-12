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

object KafkaEventStore {
  def apply(config: Configuration, accountFinder: AccountFinder[Future]): Option[(EventStore[Future], Stoppable)] = {
    for (centralZookeeperHosts <- config.get[String]("central.zk.connect")) yield {
      val serviceUID = ZookeeperSystemCoordination.extractServiceUID(config)
      val coordination = ZookeeperSystemCoordination(centralZookeeperHosts, serviceUID, true)
      val agent = serviceUID.hostId + serviceUID.serviceId

      val localConfig = config.detach("local")

      val eventIdSeq = new SystemEventIdSequence(agent, coordination)
      val Some((eventStore, esStop)) = LocalKafkaEventStore(localConfig)
      val (_, raStop) = KafkaRelayAgent(accountFinder, eventIdSeq, localConfig, config.detach("central"))

      (eventStore, esStop.parent(raStop))
    }
  }
}

object KafkaEventServer extends BlueEyesServer with EventService with AkkaDefaults {
  val clock = Clock.System
  implicit val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(defaultFutureDispatch)

  def configure(config: Configuration): (EventServiceDeps[Future], Stoppable)  = {
    val accountFinder0 = WebAccountFinder(config.detach("accounts"))

    val (eventStore0, stoppable) = KafkaEventStore(config, accountFinder) getOrElse {
      sys.error("Invalid configuration: eventStore.central.zk.connect required")
    }

    val deps = EventServiceDeps[Future]( 
      apiKeyFinder = WebAPIKeyFinder(config.detach("security")),
      accountFinder = accountFinder0,
      eventStore = eventStore0,
      jobManager = WebJobManager(config.detach("jobs"))
    )

    (deps, stoppable)
  }
}
