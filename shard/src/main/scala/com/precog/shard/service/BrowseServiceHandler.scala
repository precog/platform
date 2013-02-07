package com.precog.shard
package service

import com.precog.daze._
import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._
import com.precog.muspelheim._

import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.util.Clock

import akka.dispatch.{ Future, ExecutionContext }

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.string._
import scalaz.syntax.bifunctor._
import scalaz.syntax.validation._
import scalaz.syntax.apply._

class BrowseServiceHandler[A](metadataClient: MetadataClient[Future], accessControl: AccessControl[Future])(implicit executor: ExecutionContext)
extends CustomHttpService[A, (APIKey, Path) => Future[HttpResponse[QueryResult]]] with Logging {
  val service = (request: HttpRequest[A]) => Success({ (apiKey: APIKey, path: Path) =>
    val result: Future[Validation[(HttpStatusCode, NonEmptyList[String]), JObject]] = request.parameters.get('type).map(_.toLowerCase) match {
      case Some("size") =>
        Future((NotImplemented, NonEmptyList("Collection size metadata not yet available.")).failure[JObject])

      case Some("children") =>
        metadataClient.browse(apiKey, path) map { v =>
          {s: String => (BadRequest, NonEmptyList(s))} <-: v :-> { a: JArray => JObject("children" -> a) } 
        }

      case Some("structure") =>
        val cpath = request.parameters.get('property).map(CPath(_)).getOrElse(CPath.Identity)
        metadataClient.structure(apiKey, path, cpath) map { v => 
          {s: String => (BadRequest, NonEmptyList(s))} <-: v :-> { o: JObject => JObject("structure" -> o) } 
        }

      case _ =>
        (metadataClient.browse(apiKey, path) zip metadataClient.structure(apiKey, path, CPath.Identity)) map { 
          case (childrenV, structureV) =>
            {errs: NonEmptyList[String] => (BadRequest, errs)} <-: { 
              (childrenV.toValidationNEL |@| structureV.toValidationNEL) { (children, structure) =>
                JObject("children" -> children, "structure" -> structure)
              }
            }
        }
    }

    result map {
      _ map { jobj =>
        HttpResponse[QueryResult](OK, content = Some(Left(jobj)))
      } valueOr { 
        case (code, errors) =>
        HttpResponse[QueryResult](code, content = Some(Left(errors.list.distinct.serialize)))
      }
    }
  })

  val metadata = Some(DescriptionMetadata(
"""
Browse the children of the given path. 
"""
  ))
}
