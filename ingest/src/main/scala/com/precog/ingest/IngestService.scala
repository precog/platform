package com.precog
package ingest

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

case class IngestState(tokenManager: TokenManager, accessControl: AccessControl, eventStore: EventStore, usageLogging: UsageLogging)

trait IngestService extends BlueEyesServiceBuilder with IngestServiceCombinators { 
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  val insertTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now

  def tokenManagerFactory(config: Configuration): TokenManager
  def eventStoreFactory(config: Configuration): EventStore
  def usageLoggingFactory(config: Configuration): UsageLogging 

  val analyticsService = this.service("ingest", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          val eventStore = eventStoreFactory(config.detach("eventStore"))
          val theTokenManager = tokenManagerFactory(config.detach("security"))
          val accessControl = new TokenBasedAccessControl { 
            val tokenManager = theTokenManager  
          }

          eventStore.start map { _ =>
            IngestState(
              theTokenManager,
              accessControl,
              eventStore,
              usageLoggingFactory(config.detach("usageLogging"))
            )
          }
        } ->
        request { (state: IngestState) =>
          jsonp[ByteChunk] {
            token(state.tokenManager) {
              dataPath("track") {
                post(new TrackingServiceHandler(state.accessControl, state.eventStore, state.usageLogging, insertTimeout))
              }
            }
          }
        } ->
        shutdown { state => Future[Option[Stoppable]]( None ) }
      }
    }
  }
}
