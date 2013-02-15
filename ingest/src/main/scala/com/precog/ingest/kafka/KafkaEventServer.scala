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
import scalaz.{NonEmptyList => NEL}
import scalaz.syntax.std.option._

object KafkaEventStore {
  def apply(config: Configuration, accountFinder: AccountFinder[Future])(implicit executor: ExecutionContext): Validation[NEL[String], (EventStore[Future], Stoppable)] = {
    val localConfig = config.detach("local")
    val centralConfig = config.detach("central")

    centralConfig.get[String]("zk.connect").toSuccess(NEL("central.zk.connect configuration parameter is required")) map { centralZookeeperHosts =>
      val serviceUID = ZookeeperSystemCoordination.extractServiceUID(config)
      val coordination = ZookeeperSystemCoordination(centralZookeeperHosts, serviceUID, true)
      val agent = serviceUID.hostId + serviceUID.serviceId

      val eventIdSeq = new SystemEventIdSequence(agent, coordination)
      val Some((eventStore, esStop)) = LocalKafkaEventStore(localConfig)
      val (_, raStop) = KafkaRelayAgent(accountFinder, eventIdSeq, localConfig, centralConfig)

      (eventStore, esStop.parent(raStop))
    }
  }
}


object KafkaEventServer extends BlueEyesServer with EventService with AkkaDefaults {
  val clock = Clock.System
  implicit val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(defaultFutureDispatch)

  def configure(config: Configuration): (EventServiceDeps[Future], Stoppable)  = {
    val accountFinder0 = WebAccountFinder(config.detach("accounts")) valueOr { errs =>
      sys.error("Unable to build new WebAccountFinder: " + errs.list.mkString("\n", "\n", ""))
    }

    val (eventStore0, stoppable) = KafkaEventStore(config.detach("eventStore"), accountFinder0) valueOr { errs =>
      sys.error("Unable to build new KafkaEventStore: " + errs.list.mkString("\n", "\n", ""))
    }

    val apiKeyFinder0 = WebAPIKeyFinder(config.detach("security")) valueOr { errs =>
      sys.error("Unable to build new WebAPIKeyFinder: " + errs.list.mkString("\n", "\n", ""))
    }

    val deps = EventServiceDeps[Future]( 
      apiKeyFinder = apiKeyFinder0,
      accountFinder = accountFinder0,
      eventStore = eventStore0,
      jobManager = WebJobManager(config.detach("jobs"))
    )

    (deps, stoppable)
  }
}
