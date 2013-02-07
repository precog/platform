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
package com.precog.shard
package desktop

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import scalaz.Monad

import org.streum.configrity.Configuration

import com.precog.common.jobs.InMemoryJobManager
import com.precog.shard.jdbm3.JDBMQueryExecutorComponent
import com.precog.standalone.StandaloneShardServer

object DesktopShardServer
    extends StandaloneShardServer
    with JDBMQueryExecutorComponent {
  val caveatMessage = None
  override def hardCodedAccount = Some("desktop")

  val actorSystem = ActorSystem("desktopExecutorActorSystem")
  val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit lazy val M: Monad[Future] = new FutureMonad(asyncContext)

  def configureShardState(config: Configuration) = M.point {
    val apiKeyManager = apiKeyManagerFactory(config.detach("security"))
    val accountManager = accountManagerFactory(config.detach("accounts"))
    val jobManager = new InMemoryJobManager
    val platform = platformFactory(config.detach("queryExecutor"), apiKeyManager, accountManager, jobManager)

    val stoppable = Stoppable.fromFuture {
      for {
        _ <- apiKeyManager.close
        _ <- accountManager.close
        _ <- platform.shutdown
      } yield ()
    }

    ManagedQueryShardState(platform, apiKeyManager, accountManager, jobManager, clock, stoppable)
  } recoverWith {
    case ex: Throwable =>
      System.err.println("Could not start JDBM Shard server!!!")
      ex.printStackTrace
      Promise.failed(ex)
  }
}
