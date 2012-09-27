package com.precog
package ingest

import service._
import ingest.service._
import common.security._
import daze._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import blueeyes.bkka.AkkaDefaults
import blueeyes.bkka.Stoppable
import blueeyes.BlueEyesServiceBuilder
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.health.metrics.{eternity}

import blueeyes.core.http._
import blueeyes.json.JsonAST._

import org.streum.configrity.Configuration

import scalaz.Monad
import scalaz.syntax.monad._

case class IngestState(tokenManager: TokenManager[Future], accessControl: AccessControl[Future], eventStore: EventStore, usageLogging: UsageLogging)

trait IngestService extends BlueEyesServiceBuilder with IngestServiceCombinators with AkkaDefaults { 
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  val insertTimeout = akka.util.Timeout(10000)
  val deleteTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now
  implicit def M: Monad[Future]

  def tokenManagerFactory(config: Configuration): TokenManager[Future]
  def eventStoreFactory(config: Configuration): EventStore
  def usageLoggingFactory(config: Configuration): UsageLogging 

  val analyticsService = this.service("ingest", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          val eventStore = eventStoreFactory(config.detach("eventStore"))
          val tokenManager = tokenManagerFactory(config.detach("security"))
          val accessControl = new TokenManagerAccessControl(tokenManager)

          eventStore.start map { _ =>
            IngestState(
              tokenManager,
              accessControl,
              eventStore,
              usageLoggingFactory(config.detach("usageLogging"))
            )
          }
        } ->
        request { (state: IngestState) =>
          jsonpOrChunk {
            token(state.tokenManager) {
              path("/(?<sync>a?sync)") {
                dataPath("fs") {
                  post(new TrackingServiceHandler(state.accessControl, state.eventStore, state.usageLogging, insertTimeout, 8)(defaultFutureDispatch)) ~
                  delete(new ArchiveServiceHandler[Either[Future[JValue], ByteChunk]](state.accessControl, state.eventStore, deleteTimeout)(defaultFutureDispatch))
                }
              }
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}
