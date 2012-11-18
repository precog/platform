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
package ingest

import service._
import ingest.service._
import accounts._
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

case class EventServiceState(apiKeyManager: APIKeyManager[Future], accountManager: BasicAccountManager[Future], eventStore: EventStore, ingestPool: ExecutorService)

trait EventService extends BlueEyesServiceBuilder with EventServiceCombinators
with DecompressCombinators with AkkaDefaults { 
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  val insertTimeout = akka.util.Timeout(10000)
  val deleteTimeout = akka.util.Timeout(10000)
  implicit val timeout = akka.util.Timeout(120000) //for now
  implicit def M: Monad[Future]

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future]
  def accountManagerFactory(config: Configuration): BasicAccountManager[Future]
  def eventStoreFactory(config: Configuration): EventStore

  val eventService = this.service("ingest", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          val eventStore = eventStoreFactory(config.detach("eventStore"))
          val apiKeyManager = apiKeyManagerFactory(config.detach("security"))
          val accountManager = accountManagerFactory(config.detach("accounts"))

          // Set up a thread pool for ingest tasks
          val readPool = new ThreadPoolExecutor(config[Int]("readpool.min_threads", 2),
                                                config[Int]("readpool.max_threads", 8),
                                                config[Int]("readpool.keepalive_seconds", 5),
                                                TimeUnit.SECONDS,
                                                new ArrayBlockingQueue[Runnable](config[Int]("readpool.queue_size", 50)),
                                                new ThreadFactoryBuilder().setNameFormat("ingestpool-%d").build())

          eventStore.start map { _ =>
            EventServiceState(
              apiKeyManager,
              accountManager,
              eventStore,
              readPool
            )
          }
        } ->
        request { (state: EventServiceState) =>
          decompress {
            jsonpOrChunk {
              apiKey(state.apiKeyManager) {
                path("/(?<sync>a?sync)") {
                  dataPath("fs") {
                    accountId(state.accountManager) {
                      post(new IngestServiceHandler(state.apiKeyManager, state.eventStore, insertTimeout, state.ingestPool, maxBatchErrors = 100)(defaultFutureDispatch))
                    } ~
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
