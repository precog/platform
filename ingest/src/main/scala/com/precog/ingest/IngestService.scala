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
import blueeyes.json._

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.streum.configrity.Configuration

import scalaz.Monad
import scalaz.syntax.monad._

import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, ThreadPoolExecutor, TimeUnit}

case class IngestState(apiKeyManager: APIKeyManager[Future], eventStore: EventStore, usageLogging: UsageLogging, ingestPool: ExecutorService)

trait IngestService extends BlueEyesServiceBuilder with IngestServiceCombinators
with DecompressCombinators with AkkaDefaults { 
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  val insertTimeout = akka.util.Timeout(10000)
  val deleteTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now
  implicit def M: Monad[Future]

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future]
  def eventStoreFactory(config: Configuration): EventStore
  def usageLoggingFactory(config: Configuration): UsageLogging 

  val analyticsService = this.service("ingest", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          val eventStore = eventStoreFactory(config.detach("eventStore"))
          val apiKeyManager = apiKeyManagerFactory(config.detach("security"))

          // Set up a thread pool for ingest tasks
          val readPool = new ThreadPoolExecutor(config[Int]("readpool.min_threads", 2),
                                                config[Int]("readpool.max_threads", 8),
                                                config[Int]("readpool.keepalive_seconds", 5),
                                                TimeUnit.SECONDS,
                                                new ArrayBlockingQueue[Runnable](config[Int]("readpool.queue_size", 50)),
                                                new ThreadFactoryBuilder().setNameFormat("ingestpool-%d").build())

          eventStore.start map { _ =>
            IngestState(
              apiKeyManager,
              eventStore,
              usageLoggingFactory(config.detach("usageLogging")),
              readPool
            )
          }
        } ->
        request { (state: IngestState) =>
          decompress {
            jsonpOrChunk {
              apiKey(state.apiKeyManager) {
                path("/(?<sync>a?sync)") {
                  dataPath("fs") {
                    post(new TrackingServiceHandler(state.apiKeyManager, state.eventStore, state.usageLogging, insertTimeout, state.ingestPool, maxBatchErrors = 100)(defaultFutureDispatch)) ~
                    delete(new ArchiveServiceHandler[Either[Future[JValue], ByteChunk]](state.apiKeyManager, state.eventStore, deleteTimeout)(defaultFutureDispatch))
                  }
                }
              }
            }
          }
        } ->
        shutdown { state => 
          for {
            _ <- state.eventStore.stop
            _ <- state.apiKeyManager.close()
          } yield {
            logger.info("Stopping read threads")
            state.ingestPool.shutdown()
            if (!state.ingestPool.awaitTermination(timeout.duration.toMillis, TimeUnit.MILLISECONDS)) {
              logger.warn("Forcibly terminating remaining read threads")
              state.ingestPool.shutdownNow()
            } else {
              logger.info("Read threads stopped")
            }
            Option.empty[Stoppable]
          }
        }
      }
    }
  }
}
