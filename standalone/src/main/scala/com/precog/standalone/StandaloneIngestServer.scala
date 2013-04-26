package com.precog.standalone

import com.precog.accounts._
import com.precog.common.client._
import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.services.ServiceLocation
import com.precog.ingest._
import com.precog.ingest.service._
import com.precog.ingest.kafka._

import akka.dispatch.Future

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.core.data.ByteChunk
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.util.Clock

import com.weiglewilczek.slf4s.Logging
import org.streum.configrity.Configuration

import scalaz._

object StandaloneIngestServer extends StandaloneIngestServer with AkkaDefaults {
  val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executionContext)
  val clock = Clock.System
}

trait StandaloneIngestServer
    extends BlueEyesServer
    with EventService
    with Logging {

  def clock: Clock

  def configureEventService(config: Configuration): EventService.State  = {
    logger.debug("Starting StandaloneIngestServer with config:\n" + config)
    val apiKey = config[String]("security.masterAccount.apiKey")

    val apiKeyFinder = new StaticAPIKeyFinder[Future](apiKey)
    val accountFinder = new StaticAccountFinder[Future](config[String]("security.masterAccount.accountId"), apiKey, Some("/"))
    val permissionsFinder = new PermissionsFinder(apiKeyFinder, accountFinder, clock.instant())
    val jobManager = new InMemoryJobManager[({ type λ[+α] = EitherT[Future, String, α] })#λ]()

    val (eventStore, stoppable) = KafkaEventStore(config.detach("eventStore"), permissionsFinder) valueOr { errors =>
      sys.error("Could not configure event store: " + errors.list.mkString(", "))
    }

    val serviceConfig = EventService.ServiceConfig.fromConfiguration(config) valueOr { errors =>
      sys.error("Unable to obtain self-referential service locator for event service: %s".format(errors.list.mkString("; ")))
    }

    buildServiceState(serviceConfig, apiKeyFinder, permissionsFinder, eventStore, jobManager, stoppable)
  }
}
