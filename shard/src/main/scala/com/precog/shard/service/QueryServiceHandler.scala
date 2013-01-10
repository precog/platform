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
package com.precog.shard
package service

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s.Logging

import java.nio.CharBuffer

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.jobs._

import scalaz._
import scalaz.Validation.{ success, failure }

sealed trait QueryServiceHandler[A] extends CustomHttpService[Future[JValue], (APIKeyRecord, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]]
with Logging {
  import scalaz.syntax.monad._

  implicit def dispatcher: MessageDispatcher
  implicit def M: Monad[Future]

  def queryExecutorFactory: QueryExecutorFactory[Future, A]
  def extractResponse(a: A): HttpResponse[QueryResult]

  val Command = """:(\w+)\s+(.+)""".r

  private def handleErrors[A](qt: String, result: EvaluationError): HttpResponse[QueryResult] = result match {
    case UserError(errorData) =>
      HttpResponse[QueryResult](UnprocessableEntity, content = Some(Left(errorData)))
  
    case AccessDenied(reason) =>
      HttpResponse[QueryResult](HttpStatus(Unauthorized, reason))
  
    case TimeoutError =>
      HttpResponse[QueryResult](RequestEntityTooLarge)
  
    case SystemError(error) =>
      error.printStackTrace()
      logger.error("An error occurred processing the query: " + qt, error)
      HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))

    case InvalidStateError(error) =>
      HttpResponse[QueryResult](HttpStatus(PreconditionFailed, error))
  }

  val service = { (request: HttpRequest[Future[JValue]]) =>
    success((r: APIKeyRecord, p: Path, q: String, opts: QueryOptions) => q.trim match {
      case Command("ls", arg) => list(r.apiKey, Path(arg.trim))
      case Command("list", arg) => list(r.apiKey, Path(arg.trim))
      case Command("ds", arg) => describe(r.apiKey, Path(arg.trim))
      case Command("describe", arg) => describe(r.apiKey, Path(arg.trim))
      case qt =>
        val executorV = queryExecutorFactory.executorFor(r.apiKey)
        executorV flatMap {
          case Success(executor) =>
            executor.execute(r.apiKey, q, p, opts) map {
              case Success(result) =>
                extractResponse(result)
              case Failure(error) =>
                handleErrors(qt, error)
            }

          case Failure(error) =>
            logger.error("Failure during evaluator setup: " + error)
            Future(HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!")))
        }
    })
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))

  
  def list(apiKey: APIKey, p: Path) = {
    queryExecutorFactory.browse(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error listing path: " + p))))
    }
  }

  def describe(apiKey: APIKey, p: Path) = {
    queryExecutorFactory.structure(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error describing path: " + p))))
    }
  }
}

class SyncQueryServiceHandler(
    val queryExecutorFactory: QueryExecutorFactory[Future, StreamT[Future, CharBuffer]])(implicit
    val dispatcher: MessageDispatcher,
    val M: Monad[Future]) extends QueryServiceHandler[StreamT[Future, CharBuffer]] {

  def extractResponse(stream: StreamT[Future, CharBuffer]): HttpResponse[QueryResult] = {
    HttpResponse[QueryResult](OK, content = Some(Right(stream)))
  }
}

class AsyncQueryServiceHandler(
    val queryExecutorFactory: QueryExecutorFactory[Future, JobId])(implicit
    val dispatcher: MessageDispatcher,
    val M: Monad[Future]) extends QueryServiceHandler[JobId] {

  def extractResponse(jobId: JobId): HttpResponse[QueryResult] = {
    val result = JObject(JField("jobId", JString(jobId)) :: Nil)
    HttpResponse[QueryResult](Accepted, content = Some(Left(result)))
  }
}

