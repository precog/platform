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
package jdbc

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import scalaz.Monad

import org.streum.configrity.Configuration

import com.precog.common.jobs.JobManager
import com.precog.common.security.APIKeyFinder
import com.precog.standalone.StandaloneShardServer

object JDBCShardServer extends StandaloneShardServer {
  val caveatMessage = Some("""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Precog for PostgreSQL is a free product that Precog provides to the
PostgreSQL community for doing data analysis on PostgreSQL.

Due to technical limitations, we only recommend the product for
exploratory data analysis. For developers interested in
high-performance analytics on their PostgreSQL data, we recommend our
cloud-based analytics solution and the PostgreSQL data importer, which
can nicely complement existing PostgreSQL installations for
analytic-intensive workloads.

Please note that path globs are not yet supported in Precog for PostgreSQL
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""")

  val actorSystem = ActorSystem("ExecutorSystem")
  implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def platformFor(config: Configuration, apiKeyfinder: APIKeyFinder[Future], jobManager: JobManager[Future]) =
    (new JDBCQueryExecutor(new JDBCQueryExecutorConfig(config.detach("queryExecutor")), jobManager, actorSystem),
     Stoppable.fromFuture(Future(())))
}
