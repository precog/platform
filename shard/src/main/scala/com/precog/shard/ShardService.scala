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

case class ShardState(queryExecutor: QueryExecutor[Future], apiKeyManager: APIKeyManager[Future])

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

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future]

  val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          
          logger.info("Using config: " + config)
          logger.info("Security config = " + config.detach("security"))

          val apiKeyManager = apiKeyManagerFactory(config.detach("security"))

          logger.trace("apiKeyManager loaded")

          val queryExecutor = queryExecutorFactory(config.detach("queryExecutor"), apiKeyManager)

          logger.trace("queryExecutor loaded")

          queryExecutor.startup.map { _ =>
            ShardState(
              queryExecutor,
              apiKeyManager
            )
          }
        } ->
        request { (state: ShardState) =>
          jvalue {
            path("/actors/status") {
                get(new ActorStatusHandler(state.queryExecutor))
            }
          } ~ jsonpcb[QueryResult] {
            apiKey(state.apiKeyManager) {
              dataPath("analytics/fs") {
                query {
                  get(new QueryServiceHandler(state.queryExecutor))
                }
              } ~
              dataPath("meta/fs") {
                get(new BrowseServiceHandler(state.queryExecutor, state.apiKeyManager))
              }
            } ~ path("actors/status") {
              get(new ActorStatusHandler(state.queryExecutor))
            }
          }
        } ->
        shutdown { state =>
          for {
            shardShutdown <- state.queryExecutor.shutdown()
            _             <- state.apiKeyManager.close()
          } yield {
            logger.info("Shard system clean shutdown: " + shardShutdown)            
            Option.empty[Stoppable]
          }
        }
      }
    }
  }
}
