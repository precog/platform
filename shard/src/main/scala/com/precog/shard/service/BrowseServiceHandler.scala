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

class BrowseServiceHandler(queryExecutor: QueryExecutor[Future], accessControl: AccessControl[Future])(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (Token, Path) => Future[HttpResponse[QueryResult]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => { 
    success((t: Token, p: Path) => {
      accessControl.mayAccess(t.tid, p, Set(t.tid), ReadPermission).flatMap { 
        case true =>
          import scalaz.std.string._
          import scalaz.syntax.validation._
          import scalaz.syntax.apply._

          queryExecutor.browse(t.tid, p) flatMap { browseResult =>
            queryExecutor.structure(t.tid, p) map { structureResult =>
              (browseResult |@| structureResult) { (children, structure) =>
                JObject(
                  JField("children", children) ::
                  // JField("structure", structure) ::
                  Nil
                )
              } match {
                case Success(response) =>
                  HttpResponse[QueryResult](OK, content = Some(Left(response)))
                 case Failure(error) =>
                  HttpResponse[QueryResult](HttpStatus(BadRequest, error))
              }
            }
          }
        case false =>
          Future(HttpResponse[QueryResult](HttpStatus(Unauthorized, "The specified token may not browse this location")))
      }
    })
  }

  val metadata = Some(DescriptionMetadata(
"""
Browse the children of the given path. 
"""
  ))
}
