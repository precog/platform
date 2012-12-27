package com.precog.ingest

import service._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.security._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.BlueEyesServiceBuilder
import blueeyes.bkka.Stoppable
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.health.metrics.{eternity}
import blueeyes.json._
import blueeyes.util.Clock

import DefaultBijections._
import MimeTypes._
import ByteChunk._

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.streum.configrity.Configuration

import scalaz._
import scalaz.syntax.monad._

import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, ThreadPoolExecutor, TimeUnit}

case class EventServiceState(accessControl: APIKeyFinder[Future], ingestHandler: IngestServiceHandler, archiveHandler: ArchiveServiceHandler[ByteChunk], stop: Stoppable)

trait EventService extends BlueEyesServiceBuilder with EventServiceCombinators with DecompressCombinators { 
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def APIKeyFinder(config: Configuration): APIKeyFinder[Future]
  def AccountFinder(config: Configuration): AccountFinder[Future]
  def EventStore(config: Configuration): EventStore
  def JobManager(config: Configuration): JobManager[Future]

  val eventService = this.service("ingest", "1.0") {
    requestLogging {
      healthMonitor(defaultShutdownTimeout, List(blueeyes.health.metrics.eternity)) { monitor => context =>
        startup {
          import context._

          val eventStore = EventStore(config.detach("eventStore"))
          val apiKeyFinder = APIKeyFinder(config.detach("security"))
          val accountFinder = AccountFinder(config.detach("accounts"))
          val jobManager = JobManager(config.detach("jobs"))

          val ingestTimeout = akka.util.Timeout(config[Long]("insert.timeout", 10000l))
          val ingestBatchSize = config[Int]("ingest.batch_size", 500)

          val deleteTimeout = akka.util.Timeout(config[Long]("delete.timeout", 10000l))

          val ingestHandler = new IngestServiceHandler(accountFinder, apiKeyFinder, jobManager, Clock.System, eventStore, ingestTimeout, ingestBatchSize)
          val archiveHandler = new ArchiveServiceHandler[ByteChunk](apiKeyFinder, eventStore, deleteTimeout)

          eventStore.start map { _ =>
            EventServiceState(apiKeyFinder, ingestHandler, archiveHandler, Stoppable.fromFuture(eventStore.stop))
          }
        } ->
        request { (state: EventServiceState) =>
          decompress {
            jsonp {
              produce[ByteChunk, JValue, ByteChunk](application / json) {
                apiKey(state.accessControl) {
                  path("/(?<sync>a?sync)") {
                    dataPath("fs") {
                      post(state.ingestHandler) ~ 
                      delete(state.archiveHandler)
                    }
                  }
                }
              }
            }
          }
        } ->
        stop { state => 
          state.stop
        }
      }
    }
  }
}
