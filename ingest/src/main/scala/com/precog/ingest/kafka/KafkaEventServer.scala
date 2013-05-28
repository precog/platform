package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.client._
import com.precog.common.services.ServiceLocation
import com.precog.ingest.service._
import WebJobManager._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.util.Clock

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future }
import org.joda.time.Instant

import org.streum.configrity.Configuration

import scalaz._
import scalaz.NonEmptyList._
import scalaz.syntax.applicative._
import scalaz.syntax.std.option._

object KafkaEventServer extends BlueEyesServer with EventService with AkkaDefaults {
  val clock = Clock.System
  implicit val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(defaultFutureDispatch)

  def configureEventService(config: Configuration): EventService.State = {
    val accountFinder = new CachingAccountFinder(WebAccountFinder(config.detach("accounts")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAccountFinder: " + errs.list.mkString("\n", "\n", ""))
    })

    val apiKeyFinder = new CachingAPIKeyFinder(WebAPIKeyFinder(config.detach("security")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAPIKeyFinder: " + errs.list.mkString("\n", "\n", ""))
    })

    val permissionsFinder = new PermissionsFinder(apiKeyFinder, accountFinder, new Instant(config[Long]("ingest.timestamp_required_after", 1363327426906L)))

    val (eventStore, stoppable) = KafkaEventStore(config.detach("eventStore"), permissionsFinder) valueOr { errs =>
      sys.error("Unable to build new KafkaEventStore: " + errs.list.mkString("\n", "\n", ""))
    }

    val jobManager = WebJobManager(config.detach("jobs")) valueOr { errs =>
      sys.error("Unable to build new WebJobManager: " + errs.list.mkString("\n", "\n", ""))
    }

    val serviceConfig = EventService.ServiceConfig.fromConfiguration(config) valueOr { errors =>
      sys.error("Unable to obtain self-referential service locator for event service: %s".format(errors.list.mkString("; ")))
    }

    buildServiceState(serviceConfig, apiKeyFinder, permissionsFinder, eventStore, jobManager, stoppable)
  }
}
