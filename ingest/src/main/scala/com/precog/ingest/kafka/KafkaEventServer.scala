package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.client._
import com.precog.ingest.service._
import WebJobManager._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
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

  def configureEventService(config: Configuration): (EventServiceDeps[Future], Stoppable)  = {
    val accountFinder0 = WebAccountFinder(config.detach("accounts")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAccountFinder: " + errs.list.mkString("\n", "\n", ""))
    }

    val apiKeyFinder0 = WebAPIKeyFinder(config.detach("security")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAPIKeyFinder: " + errs.list.mkString("\n", "\n", ""))
    }

    val permissionsFinder = new PermissionsFinder(apiKeyFinder0, accountFinder0, new Instant(config[Long]("ingest.timestamp_required_after", 1363327426906L)))

    val (eventStore0, stoppable) = KafkaEventStore(config.detach("eventStore"), permissionsFinder) valueOr { errs =>
      sys.error("Unable to build new KafkaEventStore: " + errs.list.mkString("\n", "\n", ""))
    }

    val jobManager0 = WebJobManager(config.detach("jobs")) valueOr { errs =>
      sys.error("Unable to build new WebJobManager: " + errs.list.mkString("\n", "\n", ""))
    }

    val shardConf = config.detach("shard")
    val shardClient0 = (shardConf.get[String]("protocol").toSuccess(nels("Configuration property shard.protocol is required")) |@|
                        shardConf.get[String]("host").toSuccess(nels("Configuration property shard.host is required")) |@|
                        shardConf.get[Int]("port").toSuccess(nels("Configuration property shard.port is required"))) { (protocol, host, port) =>
      (new HttpClientXLightWeb).protocol(protocol).host(host).port(port)
    } valueOr { errors =>
      sys.error("Error creating proxy client for shard service: %s".format(errors.list.mkString("; ")))
    }

    val deps = EventServiceDeps[Future]( 
      apiKeyFinder = apiKeyFinder0,
      accountFinder = accountFinder0,
      eventStore = eventStore0,
      jobManager = jobManager0,
      shardClient = shardClient0
    )

    (deps, stoppable)
  }
}
