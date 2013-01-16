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

import com.precog.accounts.BasicAccountManager
import com.precog.common.jobs.JobManager

import service._
import ingest.service._
import common.Path
import common.security._
import daze._

import akka.dispatch.{Future, Promise}

import blueeyes.util.Clock
import blueeyes.json._
import blueeyes.bkka.{AkkaDefaults, Stoppable}
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.BlueEyesServiceBuilder

import blueeyes.core.data.{ DefaultBijections, ByteChunk }
import blueeyes.core.http.{HttpHeaders, HttpRequest, HttpResponse}
import blueeyes.core.service.CustomHttpService

import blueeyes.health.metrics.{eternity}
import blueeyes.json.JValue

import DefaultBijections._

import java.nio.CharBuffer

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging
import scalaz._

sealed trait BaseShardService extends 
    BlueEyesServiceBuilder with 
    ShardServiceCombinators with 
    Logging {

  type SyncQueryExecutorFactory = QueryExecutorFactory[Future, StreamT[Future, CharBuffer]]

  implicit val timeout = akka.util.Timeout(120000) //for now

  implicit def M: Monad[Future]

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future]

  def accountManagerFactory(config: Configuration): BasicAccountManager[Future]

  // TODO: maybe some of these implicits should be moved, but for now i
  // don't have the patience to figure out where.

  implicit def err: (HttpFailure, String) => HttpResponse[ByteChunk] = { (failure, s) =>
    HttpResponse(failure, content = Some(ByteChunk(s.getBytes("UTF-8"))))
  }

  implicit def failureToQueryResult: (HttpFailure, String) => HttpResponse[QueryResult] = { (fail: HttpFailure, msg: String) =>
    HttpResponse[QueryResult](status = fail, content = Some(Left(JString(msg))))
  }

  implicit def futureJValueToFutureQueryResult: Future[HttpResponse[JValue]] => Future[HttpResponse[QueryResult]] = {
    (fr: Future[HttpResponse[JValue]]) => fr.map { r => 
      r.copy(content = r.content.map(Left(_)))
    }
  }

  def optionsResponse = Promise.successful(
    HttpResponse[QueryResult](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )

  import java.nio.ByteBuffer
  import java.nio.charset.Charset

  lazy val utf8 = Charset.forName("UTF-8")

  implicit def queryResultToFutureByteChunk: QueryResult => Future[ByteChunk] = {
    (qr: QueryResult) => qr match {
      case Left(jv) => Future(Left(ByteBuffer.wrap(jv.renderCompact.getBytes)))
      case Right(stream) => Future(Right(stream.map(cb => utf8.encode(cb))))
    }
  }

  implicit def futureByteChunk2byteChunk: Future[ByteChunk] => ByteChunk = { fb =>
    Right(StreamT.wrapEffect[Future, ByteBuffer] {
      fb map {
        case Left(buffer) => buffer :: StreamT.empty[Future, ByteBuffer]
        case Right(stream) => stream
      }
    })
  }

  implicit def queryResult2byteChunk = futureByteChunk2byteChunk compose queryResultToFutureByteChunk

  // I feel dirty inside.
  implicit def futureMapper[A, B](implicit a2b: A => B): Future[HttpResponse[A]] => Future[HttpResponse[B]] = { fa =>
    fa map { res => res.copy(content = res.content map a2b) }
  }

  def asyncQueryService(queryExecutorFactory: AsyncQueryExecutorFactory, apiKeyManager: APIKeyManager[Future], jobManager: JobManager[Future], clock: Clock) = {
    apiKey[ByteChunk, HttpResponse[ByteChunk]](apiKeyManager) {
      path("/analytics/queries") {
        path("'jobId") {
          get(new AsyncQueryResultServiceHandler(jobManager)) ~
          delete(new QueryDeleteHandler(jobManager, clock))
        }
      }
    } ~
    apiKey(apiKeyManager) {
      path("/analytics/queries") {
        asyncQuery {
          post(new AsyncQueryServiceHandler(queryExecutorFactory.asynchronous))
        }
      }
    }
  }

  def basicQueryService(queryExecutorFactory: SyncQueryExecutorFactory, apiKeyManager: APIKeyManager[Future]) = {
    jvalue[ByteChunk] {
      path("/actors/status") {
        get(new ActorStatusHandler(queryExecutorFactory))
      }
    } ~ jsonpcb[QueryResult] {
      apiKey(apiKeyManager) {
        dataPath("analytics/fs") {
          query {
            get(new SyncQueryServiceHandler(queryExecutorFactory)) ~
            // Handle OPTIONS requests internally to simplify the standalone service
            options {
              (request: HttpRequest[ByteChunk]) => {
                (a: APIKeyRecord, p: Path, s: String, o: QueryOptions) => optionsResponse
              }
            }
          }
        } ~
        dataPath("meta/fs") {
          get(new BrowseServiceHandler(queryExecutorFactory, apiKeyManager)) ~
          // Handle OPTIONS requests internally to simplify the standalone service
          options {
            (request: HttpRequest[ByteChunk]) => {
              (a: APIKeyRecord, p: Path) => optionsResponse
            }
          }
        }
      } ~ path("actors/status") {
        get(new ActorStatusHandler(queryExecutorFactory))
      }
    }
  }
}

trait ShardService extends BaseShardService {

  case class ShardState(
    queryExecutorFactory: SyncQueryExecutorFactory,
    apiKeyManager: APIKeyManager[Future]
  )

  def queryExecutorFactoryFactory(config: Configuration,
    accessControl: APIKeyManager[Future], //FIXME! This is awful
    accountManager: BasicAccountManager[Future]): SyncQueryExecutorFactory

  lazy val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          logger.info("Using config: " + config)

          val apiKeyManager = apiKeyManagerFactory(config.detach("security"))

          logger.trace("apiKeyManager loaded")

          val accountManager = accountManagerFactory(config.detach("accounts"))

          logger.trace("accountManager loaded")

          val queryExecutorFactory = queryExecutorFactoryFactory(config.detach("queryExecutor"), apiKeyManager, accountManager)

          logger.trace("queryExecutorFactory loaded")

          queryExecutorFactory.startup.map { _ =>
            ShardState(queryExecutorFactory, apiKeyManager)
          }
        } ->
        request { case ShardState(queryExecutorFactory, apiKeyManager) =>
          basicQueryService(queryExecutorFactory, apiKeyManager)
        } ->
        shutdown { case ShardState(queryExecutorFactory, apiKeyManager) =>
          for {
            shutdownState <- queryExecutorFactory.shutdown()
            _ <- apiKeyManager.close()
          } yield {
            logger.info("Stopped shard: " + shutdownState)
            Option.empty[Stoppable]
          }
        }
      }
    }
  }
}

trait AsyncShardService extends BaseShardService {

  case class ShardState(
    queryExecutorFactory: AsyncQueryExecutorFactory,
    apiKeyManager: APIKeyManager[Future],
    jobManager: JobManager[Future],
    clock: Clock)

  def clock: Clock

  def jobManagerFactory(config: Configuration): JobManager[Future] 

  def queryExecutorFactoryFactory(config: Configuration,
    accessControl: APIKeyManager[Future], //FIXME! THIS IS AWFUL
    accountManager: BasicAccountManager[Future],
    jobManager: JobManager[Future]): AsyncQueryExecutorFactory

  lazy val analyticsService = this.service("quirrel", "1.0") {
    requestLogging(timeout) {
      healthMonitor(timeout, List(eternity)) { monitor => context =>
        startup {
          import context._

          logger.info("Using config: " + config)

          val apiKeyManager = apiKeyManagerFactory(config.detach("security"))

          logger.trace("apiKeyManager loaded")

          val accountManager = accountManagerFactory(config.detach("accounts"))

          logger.trace("accountManager loaded")

          val jobManager = jobManagerFactory(config.detach("jobs"))

          logger.trace("jobManager loaded")

          val queryExecutorFactory = queryExecutorFactoryFactory(config.detach("queryExecutor"), apiKeyManager, accountManager, jobManager)

          logger.trace("queryExecutorFactory loaded")

          queryExecutorFactory.startup.map { _ =>
            ShardState(queryExecutorFactory, apiKeyManager, jobManager, clock)
          }
        } ->
        request { case ShardState(queryExecutorFactory, apiKeyManager, jobManager, clock) =>
          asyncQueryService(queryExecutorFactory, apiKeyManager, jobManager, clock) ~
          basicQueryService(queryExecutorFactory, apiKeyManager)
        } ->
        shutdown { state =>
          for {
            shutdownState <- state.queryExecutorFactory.shutdown()
            _ <- state.apiKeyManager.close()
          } yield {
            logger.info("Stopped shard: " + shutdownState)
            Option.empty[Stoppable]
          }
        }
      }
    }
  }
}

