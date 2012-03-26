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
  val service = (request: HttpRequest[Future[JValue]]) => { 
    success((t: Token, p: Path, q: String) => 
      Future(queryExecutor.execute(t.uid, q) match {
        case Success(result)               => HttpResponse[JValue](OK, content = Some(result))
        case Failure(UserError(errorData)) => HttpResponse[JValue](UnprocessableEntity, content = Some(errorData))
        case Failure(AccessDenied(reason)) => HttpResponse[JValue](HttpStatus(Unauthorized, reason))
        case Failure(TimeoutError)         => HttpResponse[JValue](RequestEntityTooLarge)
        case Failure(SystemError(error))   => 
          logger.error("An error occurred processing the query: " + q, error)
          HttpResponse[JValue](HttpStatus(InternalServerError, "A problem was encountered processing your query. We're looking into it!"))
      }))
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))
}
