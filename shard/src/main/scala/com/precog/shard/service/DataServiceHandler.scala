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

import com.precog.common.Path
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.services.ServiceHandlerUtil
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.vfs.Version

import blueeyes.util.Clock
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.json._

import akka.dispatch.{ ExecutionContext, Future }
import com.weiglewilczek.slf4s.Logging

import java.nio.ByteBuffer

import scalaz._
import scalaz.syntax.show._
import scalaz.effect.IO

// having access to the whole platform is a bit of overkill, but whatever
class DataServiceHandler[A](platform: Platform[Future, Slice, StreamT[Future, Slice]])(implicit M: Monad[Future])
    extends CustomHttpService[A, (APIKey, Path) => Future[HttpResponse[ByteChunk]]] with Logging {

  val service = (request: HttpRequest[A]) => Success {
    (apiKey: APIKey, path: Path) => {
      val mimeTypes = request.headers.header[Accept].toSeq.flatMap(_.mimeTypes)
      platform.vfs.readResource(apiKey, path, Version.Current, AccessMode.Read).run flatMap {
        _.fold(
          error => {
            logger.error("Read failure: " + error.shows)
            sys.error("fixme... return an informative HTTP repsonse.")
          },
          resource => resource.byteStream(mimeTypes).run map {
            case Some((reportedType, byteStream)) =>
              HttpResponse(OK, headers = HttpHeaders(`Content-Type`(reportedType)), content = Some(Right(byteStream)))

            case None =>
              HttpResponse(NotFound, content = Some(Left(("Could not locate content for path " + path).getBytes("UTF-8"))))
          }
        )
      } recover {
        case ex => 
            logger.error("Exception thrown in readResource evaluation.", ex)
            HttpResponse(InternalServerError)
      }
    }
  }

  val metadata = NoMetadata //FIXME
}
