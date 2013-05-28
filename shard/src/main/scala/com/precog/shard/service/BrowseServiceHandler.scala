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
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.vfs._
import ResourceError._

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
import scalaz.syntax.show._
import scalaz.syntax.apply._

class BrowseSupport[M[+_]: Bind](vfs: VFSMetadata[M]) {
  def size(apiKey: APIKey, path: Path): EitherT[M, ResourceError, JNum] =
    vfs.size(apiKey, path, Version.Current) map { JNum(_) }

  def browse(apiKey: APIKey, path: Path): EitherT[M, ResourceError, JArray] = {
    vfs.findDirectChildren(apiKey, path) map { paths =>
      JArray(paths.map(p => JString(p.toString.substring(1))).toSeq: _*)
    }
  }

  def structure(apiKey: APIKey, path: Path, property: CPath): EitherT[M, ResourceError, JObject] = {
    /**
     * This turns a set of types/counts into something usable by strucutre. It
     * will serialize the longs to JNums and unify CNumericTypes under "Number".
     */
    def normalizeTypes(xs: Map[CType, Long]): Map[String, JValue] = {
      xs.foldLeft(Map.empty[String, Long]) {
        case (acc, ((CLong | CDouble | CNum), count)) =>
          acc + ("Number" -> (acc.getOrElse("Number", 0L) + count))
        case (acc, (ctype, count)) =>
          acc + (CType.nameOf(ctype) -> count)
      } mapValues (_.serialize)
    }

    vfs.pathStructure(apiKey, path, property, Version.Current) map {
      case PathStructure(types, children) =>
        JObject(Map("children" -> children.serialize,
                    "types" -> JObject(normalizeTypes(types))))
    }
  }
}

class BrowseServiceHandler[A](vfs0: VFSMetadata[Future])(implicit M: Monad[Future])
    extends BrowseSupport[Future](vfs0) with CustomHttpService[A, (APIKey, Path) => Future[HttpResponse[JValue]]] with Logging {

  val service = (request: HttpRequest[A]) => success { (apiKey: APIKey, path: Path) =>
    request.parameters.get('type).map(_.toLowerCase) map {
      case "size" =>
        size(apiKey, path) map { sz => JObject("size" -> sz) }

      case "children" =>
        browse(apiKey, path) map { paths => JObject("children" -> paths) }

      case "structure" =>
        val cpath = request.parameters.get('property).map(CPath(_)).getOrElse(CPath.Identity)
        structure(apiKey, path, cpath) map { detail => JObject("structure" -> detail) }
    } getOrElse {
      logger.debug("Retrieving all available metadata for %s as %s".format(path.path, apiKey))
      for {
        sz <- size(apiKey, path)
        children <- browse(apiKey, path)
        struct <- structure(apiKey, path, CPath.Identity)
      } yield {
        JObject("size" -> sz, "children" -> children, "structure" -> struct)
      }
    } map { content0 =>
      HttpResponse[JValue](OK, content = Some(content0))
    } valueOr { 
      _.fold(
        fatalError => {
          logger.error("A fatal error was encountered handling browse request %s: %s".format(request.shows, fatalError))
          HttpResponse[JValue](InternalServerError, content = Some(JObject("errors" -> JArray("sorry, we're looking into it!".serialize))))
        },
        {
          case ResourceError.NotFound(message) =>
            HttpResponse[JValue](HttpStatusCodes.NotFound, content = Some(JObject("errors" -> JArray("Could not find any resource that corresponded to path %s: %s".format(path.path, message).serialize))))

          case PermissionsError(message) =>
            HttpResponse[JValue](Forbidden, content = Some(JObject("errors" -> JArray("API key %s does not have the ability to browse path %s: %s".format(apiKey, path.path, message).serialize))))

          case unexpected =>
            logger.error("An unexpected error was encountered handling browse request %s: %s".format(request.shows, unexpected))
            HttpResponse[JValue](InternalServerError, content = Some(JObject("errors" -> "sorry, we're looking into it!".serialize)))
        }
      )
    }
  }

  val metadata = DescriptionMetadata("""Browse the children of the given path.""")
}
