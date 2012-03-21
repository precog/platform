package com.precog
package shard

import service._
import ingest.service._
import common.security._
import daze._

import akka.dispatch.Future

import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.BlueEyesServiceBuilder
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.health.metrics.{eternity}

import org.streum.configrity.Configuration

case class ShardState(queryExecutor: QueryExecutor, tokenManager: TokenManager, accessControl: AccessControl, usageLogging: UsageLogging)

trait ShardService extends BlueEyesServiceBuilder with IngestServiceCombinators with AkkaDefaults {
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

          val queryExecutor = queryExecutorFactory(config.detach("queryExecutor"))

          val theTokenManager = tokenManagerFactory(config.detach("security"))

          val accessControl = new TokenBasedAccessControl {
            val executionContext = defaultFutureDispatch
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
