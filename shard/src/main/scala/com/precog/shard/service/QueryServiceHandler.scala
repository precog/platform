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

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import scalaz.Success
import scalaz.Failure

import com.weiglewilczek.slf4s.Logging

import com.precog.analytics.Token

class QueryServiceHandler(queryExecutor: QueryExecutor)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {

  import QueryServiceHandler._

  val service = (request: HttpRequest[Future[JValue]]) => { 
    Success{ (t: Token) => 
      if(!t.expired) {
        request.content.map { _.map { 
          case JString(s) => 
            val queryResult = queryExecutor.execute(s)
            HttpResponse[JValue](OK, content=Some(queryResult))

          case _          => InvalidQuery 
        }}.getOrElse( Future { InvalidQuery } )
      } else {
        Future { ExpiredToken }
      }
    }
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))
}

object QueryServiceHandler {
  val InvalidQuery: HttpResponse[JValue] = toResponse(BadRequest, "Expected query as json string.")
  val ExpiredToken: HttpResponse[JValue] = toResponse(Unauthorized, "Your token has expired.")

  def toResponse(status: HttpStatusCode, msg: String): HttpResponse[JValue] = 
    HttpResponse[JValue](status, content=Some(JString(msg)))
}
