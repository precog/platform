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
import scalaz.Validation._
import scalaz.std.string._
import scalaz.syntax.bifunctor._
//import scalaz.syntax.validation._
import scalaz.syntax.apply._

class BrowseServiceHandler[A](metadataClient: MetadataClient[Future], accessControl: AccessControl[Future])(implicit executor: ExecutionContext)
extends CustomHttpService[A, (APIKey, Path) => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[A]) => success { (apiKey: APIKey, path: Path) =>
    request.parameters.get('type).map(_.toLowerCase) map {
      case "size" =>
        metadataClient.size(apiKey, path) map { v =>
          {s: String => (InternalServerError, NonEmptyList(s))} <-: v :-> { a: JNum => JObject("size" -> a) }
        }

      case "children" =>
        metadataClient.browse(apiKey, path) map { v =>
          {s: String => (InternalServerError, NonEmptyList(s))} <-: v :-> { a: JArray => JObject("children" -> a) }
        }

      case "structure" =>
        val cpath = request.parameters.get('property).map(CPath(_)).getOrElse(CPath.Identity)
        metadataClient.structure(apiKey, path, cpath) map { v =>
          {s: String => (InternalServerError, NonEmptyList(s))} <-: v :-> { o: JObject => JObject("structure" -> o) }
        }
    } getOrElse {
      (metadataClient.size(apiKey, path) zip metadataClient.browse(apiKey, path) zip metadataClient.structure(apiKey, path, CPath.Identity)) map {
        case ((sizeV, childrenV), structureV) =>
          {errs: NonEmptyList[String] => (InternalServerError, errs)} <-: {
            (sizeV.toValidationNel |@| childrenV.toValidationNel |@| structureV.toValidationNel) { (size, children, structure) =>
              JObject("size" -> size, "children" -> children, "structure" -> structure)
            }
          }
      }
    } map {
      case Success(jobj) =>
        HttpResponse[JValue](OK, content = Some(jobj))
      case Failure((code, errors)) =>
        HttpResponse[JValue](code, content = Some(JObject("errors" -> errors.list.distinct.serialize)))
    }
  }

  val metadata = DescriptionMetadata("""Browse the children of the given path.""")
}
