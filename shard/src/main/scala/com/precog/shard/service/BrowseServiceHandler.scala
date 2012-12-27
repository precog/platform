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

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.string._
import scalaz.syntax.validation._
import scalaz.syntax.apply._

class BrowseServiceHandler[A](metadataClient: MetadataClient[Future], accessControl: AccessControl[Future])(implicit M: Monad[Future])
extends CustomHttpService[A, (APIKey, Path) => Future[HttpResponse[QueryResult]]] with Logging {
  val service = (request: HttpRequest[A]) => { 
    Success((apiKey: APIKey, path: Path) => {
      metadataClient.browse(apiKey, path) flatMap { browseResult => browseResult match {
        case Success(_) =>
          metadataClient.structure(apiKey, path) map { structureResult =>
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
          M.point(HttpResponse[QueryResult](HttpStatus(BadRequest, error)))
      }}
    })
  }

  val metadata = Some(DescriptionMetadata(
"""
Browse the children of the given path. 
"""
  ))
}
