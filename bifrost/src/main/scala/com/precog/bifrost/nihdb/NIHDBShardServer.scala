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
package com.precog.bifrost
package nihdb

import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.client._
import com.precog.yggdrasil.scheduling._
import com.precog.yggdrasil.vfs._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.actor.{ActorSystem, Props}
import akka.pattern.GracefulStopSupport
import akka.util.Timeout

import java.util.concurrent.TimeUnit

import org.joda.time.Instant
import org.streum.configrity.Configuration

import scalaz._

object NIHDBShardServer extends BlueEyesServer
    with ShardService
    with NIHDBQueryExecutorComponent {
  import WebJobManager._

  val clock = Clock.System

  val actorSystem = ActorSystem("PrecogShard")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  override def configureShardState(config: Configuration) = M.point {
    val apiKeyFinder = new CachingAPIKeyFinder(WebAPIKeyFinder(config.detach("security")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAPIKeyFinder: " + errs.list.mkString("\n", "\n", ""))
    })

    val accountFinder = new CachingAccountFinder(WebAccountFinder(config.detach("accounts")).map(_.withM[Future]) valueOr { errs =>
      sys.error("Unable to build new WebAccountFinder: " + errs.list.mkString("\n", "\n", ""))
    })

    val (asyncQueries, jobManager) = {
      if (config[Boolean]("jobs.service.in_memory", false)) {
        (ShardStateOptions.DisableAsyncQueries, ExpiringJobManager[Future](config.detach("jobs")))
      } else {
        WebJobManager(config.detach("jobs")) map { webJobManager =>
          (ShardStateOptions.NoOptions, webJobManager.withM[Future])
        } valueOr { errs =>
          sys.error("Unable to build new WebJobManager: " + errs.list.mkString("\n", "\n", ""))
        }
      }
    }

    val platform = nihdbPlatform(config, apiKeyFinder, accountFinder, jobManager)

    ShardState(platform, apiKeyFinder, accountFinder, platform.scheduler, jobManager, clock, Stoppable.fromFuture(platform.shutdown), asyncQueries)
  } recoverWith {
    case ex: Throwable =>
      System.err.println("Could not start NIHDB Shard server!!!")
      ex.printStackTrace
      Promise.failed(ex)
  }
}
