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
import com.precog.common._

class QueryServiceHandler(queryExecutor: QueryExecutor)(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], Token => Future[HttpResponse[JValue]]] with Logging {

  import QueryServiceHandler._

  val service = (request: HttpRequest[Future[JValue]]) => { 
    Success{ (t: Token) => 
      if(!t.expired) {
        request.content.map { _.map { 
          case JString(s) => 
            val queryResult = queryExecutor.execute(t.tokenId, s)
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
