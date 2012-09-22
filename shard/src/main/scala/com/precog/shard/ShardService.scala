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
import blueeyes.core.data.{ BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk }
import blueeyes.health.metrics.{eternity}

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._

case class ShardState(queryExecutor: QueryExecutor[Future], tokenManager: TokenManager[Future], accessControl: AccessControl[Future])

trait ShardService extends 
    BlueEyesServiceBuilder with 
    ShardServiceCombinators with 
    AkkaDefaults with 
    Logging {
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._
  import BijectionsChunkQueryResult._

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def M: Monad[Future]

  def queryExecutorFactory(config: Configuration, accessControl: AccessControl[Future]): QueryExecutor[Future]

  def tokenManagerFactory(config: Configuration): TokenManager[Future]

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          
          logger.info("Using config: " + config)
          logger.info("Security config = " + config.detach("security"))

          val tokenManager = tokenManagerFactory(config.detach("security"))

          logger.trace("tokenManager loaded")

          val accessControl = new TokenManagerAccessControl(tokenManager)

          logger.trace("accessControl loaded")
          
          val queryExecutor = queryExecutorFactory(config.detach("queryExecutor"), accessControl)

          logger.trace("queryExecutor loaded")

          queryExecutor.startup.map { _ =>
            ShardState(
              queryExecutor,
              tokenManager,
              accessControl
            )
          }
        } ->
        request { (state: ShardState) =>
          jvalue {
            path("/actors/status") {
                get(new ActorStatusHandler(state.queryExecutor))
            }
          } ~ jsonpcb[QueryResult] {
            token(state.tokenManager) {
              dataPath("analytics/fs") {
                query {
                  get(new QueryServiceHandler(state.queryExecutor))
                }
              } ~
              dataPath("meta/fs") {
                get(new BrowseServiceHandler(state.queryExecutor, state.accessControl))
              }
            } ~ path("actors/status") {
              get(new ActorStatusHandler(state.queryExecutor))
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}
