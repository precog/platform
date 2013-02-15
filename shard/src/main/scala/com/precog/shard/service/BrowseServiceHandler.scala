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
          {s: String => (InternalServerError, NonEmptyList(s))} <-: v :-> { a: JArray => JObject("children" -> a) } 
        }

      case Some("structure") =>
        val cpath = request.parameters.get('property).map(CPath(_)).getOrElse(CPath.Identity)
        metadataClient.structure(apiKey, path, cpath) map { v => 
          {s: String => (InternalServerError, NonEmptyList(s))} <-: v :-> { o: JObject => JObject("structure" -> o) } 
        }

      case _ =>
        (metadataClient.browse(apiKey, path) zip metadataClient.structure(apiKey, path, CPath.Identity)) map { 
          case (childrenV, structureV) =>
            {errs: NonEmptyList[String] => (InternalServerError, errs)} <-: { 
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
