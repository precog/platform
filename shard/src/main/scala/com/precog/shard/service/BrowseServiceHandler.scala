package com.precog.shard
package service

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
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

import scalaz.std.string._
import scalaz.syntax.validation._
import scalaz.syntax.apply._

class BrowseServiceHandler(queryExecutor: QueryExecutor[Future], accessControl: AccessControl[Future])(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (APIKeyRecord, Path) => Future[HttpResponse[QueryResult]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => { 
    success((r: APIKeyRecord, p: Path) => {
      queryExecutor.browse(r.apiKey, p) flatMap { browseResult => browseResult match {
        case Success(_) =>
          queryExecutor.structure(r.apiKey, p) map { structureResult =>
            (browseResult |@| structureResult) { (children, structure) =>
              JObject(
                JField("children", children) ::
                Nil
              )
            } match {
              case Success(response) =>
                HttpResponse[QueryResult](OK, content = Some(Left(response)))
               case Failure(error) =>
                HttpResponse[QueryResult](HttpStatus(BadRequest, error))
            }
          }
        case Failure(error) =>
          Future(HttpResponse[QueryResult](HttpStatus(BadRequest, error)))
      }}
    })
  }

  val metadata = Some(DescriptionMetadata(
"""
Browse the children of the given path. 
"""
  ))
}
