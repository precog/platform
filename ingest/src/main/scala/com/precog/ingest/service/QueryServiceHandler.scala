package com.precog.ingest
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
 
  private val InvalidQuery = HttpResponse[JValue](BadRequest, content=Some(JString("Expected query as json string.")))

  val service = (request: HttpRequest[Future[JValue]]) => { 
    Success{ (t: Token) => request.content.map { _.map { 
      case JString(s) => 
        val queryResult = queryExecutor.execute(s)
        HttpResponse[JValue](OK, content=Some(queryResult))

      case _          => InvalidQuery 
    }}.getOrElse( Future { InvalidQuery } ) }
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))
}
