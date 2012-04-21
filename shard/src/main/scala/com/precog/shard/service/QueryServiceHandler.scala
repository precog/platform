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

  val Command = """:(\w+)\(([^)]*)\)""".r 

  val service = (request: HttpRequest[Future[JValue]]) => { 
    success((t: Token, p: Path, q: String) => 
      if(t.expired) {
        Future(HttpResponse[JValue](HttpStatus(Unauthorized, "The specified token has expired")))
      } else if(p != Path("/")) {
        Future(HttpResponse[JValue](HttpStatus(Unauthorized, "Queries made at non-root paths are not yet available.")))
      } else {
        q.trim match {
          case Command("browse", arg) => 
            queryExecutor.browse(t.uid, Path(arg)).map {
              case Success(r) => HttpResponse[JValue](OK, content = Some(r))
              case Failure(e) => HttpResponse[JValue](BadRequest, content = Some(JString("Error browsing path: " + arg)))
            }
          case Command("structure", arg) =>
            queryExecutor.structure(t.uid, Path(arg)).map {
              case Success(r) => HttpResponse[JValue](OK, content = Some(r))
              case Failure(e) => HttpResponse[JValue](BadRequest, content = Some(JString("Error showing structure for path: " + arg)))
            }
          case qt =>
            Future(queryExecutor.execute(t.uid, qt) match {
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
      })
  }

  val metadata = Some(DescriptionMetadata(
    """
Takes a quirrel query and returns the result of evaluating the query.
    """
  ))
}
