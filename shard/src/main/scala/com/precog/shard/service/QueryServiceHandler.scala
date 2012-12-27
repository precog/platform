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

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._

import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s.Logging

import scalaz.{ Monad, Success, Failure }
import scalaz.Validation._
import scalaz.syntax.monad._


class QueryServiceHandler[A](queryExecutor: QueryExecutor[Future])(implicit M: Monad[Future])
extends CustomHttpService[A, (APIKey, Path, String, QueryOptions) => Future[HttpResponse[QueryResult]]]
with Logging {
  val Command = """:(\w+)\s+(.+)""".r

  val service = (request: HttpRequest[A]) => {
    success((apiKey: APIKey, path: Path, query: String, opts: QueryOptions) => query.trim match {
      case Command("ls", arg) => list(apiKey, Path(arg.trim))
      case Command("list", arg) => list(apiKey, Path(arg.trim))
      case Command("ds", arg) => describe(apiKey, Path(arg.trim))
      case Command("describe", arg) => describe(apiKey, Path(arg.trim))
      case qt =>
        queryExecutor.execute(apiKey, query, path, opts) match {
          case Success(stream) =>
            M.point(HttpResponse[QueryResult](OK, content = Some(Right(stream))))
          
          case Failure(UserError(errorData)) =>
            M.point(HttpResponse[QueryResult](UnprocessableEntity, content = Some(Left(errorData))))
          
          case Failure(AccessDenied(reason)) =>
            M.point(HttpResponse[QueryResult](HttpStatus(Unauthorized, reason)))
          
          case Failure(TimeoutError) => 
            M.point(HttpResponse[QueryResult](RequestEntityTooLarge))
          
          case Failure(SystemError(error)) =>
            error.printStackTrace()
            logger.error("An error occurred processing the query: " + qt, error)
            M.point(HttpResponse[QueryResult](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!")))
        }
    })
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))

  
  def list(apiKey: APIKey, p: Path) = {
    queryExecutor.browse(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error listing path: " + p))))
    }
  }

  def describe(apiKey: APIKey, p: Path) = {
    queryExecutor.structure(apiKey, p).map {
      case Success(r) => HttpResponse[QueryResult](OK, content = Some(Left(r)))
      case Failure(e) => HttpResponse[QueryResult](BadRequest, content = Some(Left(JString("Error describing path: " + p))))
    }
  }
}
