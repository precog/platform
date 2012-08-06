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
import blueeyes.json.JsonAST._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import scalaz.Success
import scalaz.Failure
import scalaz.Validation._

import com.weiglewilczek.slf4s.Logging

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._

class QueryServiceHandler(queryExecutor: QueryExecutor)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path, String) => Future[HttpResponse[JValue]]] with Logging {

  val Command = """:(\w+)\s+(.+)""".r 

  val service = (request: HttpRequest[Future[JValue]]) => { 
    success((t: Token, p: Path, q: String) => 
     // if(p != Path("/")) {
      //  Future(HttpResponse[JValue](HttpStatus(Unauthorized, "Queries made at non-root paths are not yet available.")))
     // } else {
        q.trim match {
          case Command("ls", arg) => list(t.tid, Path(arg.trim))
          case Command("list", arg) => list(t.tid, Path(arg.trim))
          case Command("ds", arg) => describe(t.tid, Path(arg.trim))
          case Command("describe", arg) => describe(t.tid, Path(arg.trim))
          case qt =>
            Future(queryExecutor.execute(t.tid, qt, p) match {
              case Success(result)               => HttpResponse[JValue](OK, content = Some(result))
              case Failure(UserError(errorData)) => HttpResponse[JValue](UnprocessableEntity, content = Some(errorData))
              case Failure(AccessDenied(reason)) => HttpResponse[JValue](HttpStatus(Unauthorized, reason))
              case Failure(TimeoutError)         => HttpResponse[JValue](RequestEntityTooLarge)
              case Failure(SystemError(error))   => 
                error.printStackTrace() 
                logger.error("An error occurred processing the query: " + qt, error)
                HttpResponse[JValue](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))
            })
        }
      //}
      )

  }
  
  def list(u: UID, p: Path) = {
    queryExecutor.browse(u, p).map {
      case Success(r) => HttpResponse[JValue](OK, content = Some(r))
      case Failure(e) => HttpResponse[JValue](BadRequest, content = Some(JString("Error listing path: " + p)))
    }
  }

  def describe(u: UID, p: Path) = {
    queryExecutor.structure(u, p).map {
      case Success(r) => HttpResponse[JValue](OK, content = Some(r))
      case Failure(e) => HttpResponse[JValue](BadRequest, content = Some(JString("Error describing path: " + p)))
    }
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))
}
