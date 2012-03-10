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
package com.precog
package shard

import service._
import ingest.service._
import common.security._
import daze._

import akka.dispatch.Future

import blueeyes.bkka.Stoppable
import blueeyes.BlueEyesServiceBuilder
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.health.metrics.{eternity}

import org.streum.configrity.Configuration

case class ShardState(queryExecutor: QueryExecutor, tokenManager: TokenManager, accessControl: AccessControl, usageLogging: UsageLogging)

trait ShardService extends BlueEyesServiceBuilder with IngestServiceCombinators {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  implicit val timeout = akka.util.Timeout(120000) //for now

  def queryExecutorFactory(config: Configuration): QueryExecutor

  def usageLoggingFactory(config: Configuration): UsageLogging 

  def tokenManagerFactory(config: Configuration): TokenManager

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          val queryExecutor = queryExecutorFactory(config.detach("query_executor"))

          val theTokenManager = tokenManagerFactory(config.detach("security"))

          val accessControl = new TokenBasedAccessControl {
            val tokenManager = theTokenManager
          }

          queryExecutor.startup.map { _ =>
            ShardState(
              queryExecutor,
              theTokenManager,
              accessControl,
              usageLoggingFactory(config.detach("usageLogging"))
            )
          }
        } ->
        request { (state: ShardState) =>
          jsonp[ByteChunk] {
            token(state.tokenManager) {
              path("/query") {
                post(new QueryServiceHandler(state.queryExecutor))
              }
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}
