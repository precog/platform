package com.precog.shard
package service

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s.Logging

import com.precog.daze._
import com.precog.common._
import com.precog.common.security._
import com.precog.muspelheim._

import scalaz._
import scalaz.std.string._
import scalaz.syntax.validation._
import scalaz.syntax.apply._

class BrowseServiceHandler(metadataClient: MetadataClient[Future], accessControl: AccessControl[Future])(implicit dispatcher: MessageDispatcher)
extends CustomHttpService[Future[JValue], (APIKeyRecord, Path) => Future[HttpResponse[QueryResult]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => { 
    Success((r: APIKeyRecord, p: Path) => {
      metadataClient.browse(r.apiKey, p) flatMap { browseResult => browseResult match {
        case Success(_) =>
          metadataClient.structure(r.apiKey, p) map { structureResult =>
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
