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
    val accountFinder0 = WebAccountFinder(config.detach("accounts")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAccountFinder: " + errs.list.mkString("\n", "\n", ""))
    }

    val (eventStore0, stoppable) = KafkaEventStore(config.detach("eventStore"), accountFinder0) valueOr { errs =>
      sys.error("Unable to build new KafkaEventStore: " + errs.list.mkString("\n", "\n", ""))
    }

    val apiKeyFinder0 = WebAPIKeyFinder(config.detach("security")).map(_.withM[Future]) valueOr { errs =>
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
