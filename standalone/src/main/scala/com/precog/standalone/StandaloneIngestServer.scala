/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
