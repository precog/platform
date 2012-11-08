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
package com.precog.ingest
package service

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.precog.common.Path
import com.precog.common.security._

trait IngestServiceCombinators extends APIKeyServiceCombinators {

  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  def left[A, B, C](h: HttpService[Either[A, B], C]): HttpService[A, C] = {
    new CustomHttpService[A, C] {
      val service = (req: HttpRequest[A]) =>
        h.service(req.copy(content = req.content map (Left(_))))

      val metadata = None
    }
  }

  def right[A, B, C](h: HttpService[Either[A, B], C]): HttpService[B, C] = {
    new CustomHttpService[B, C] {
      val service = (req: HttpRequest[B]) =>
        h.service(req.copy(content = req.content map (Right(_))))

      val metadata = None
    }
  }

  /**
   * If the request has no content, then it is treated as a JSON-P request and
   * the `content` paramter is given as the content of the JSON-P request as a
   * `Future[JValue]`. Otherwise, the content is streamed in as a `ByteChunk`.
   */
  def jsonpOrChunk(h: HttpService[Either[Future[JValue], ByteChunk], Future[HttpResponse[JValue]]]) = {
    new CustomHttpService[ByteChunk, Future[HttpResponse[ByteChunk]]] {
      val service = (request: HttpRequest[ByteChunk]) => {
        (if (request.content.isEmpty) {
          jsonp[ByteChunk](left(h))
        } else {
          produce(MimeTypes.application/MimeTypes.json)(right(h))
        }).service(request)
      }

      val metadata = None
    }
  }

  def dataPath[A, B](prefix: String)(next: HttpService[A, (APIKeyRecord, Path) => Future[B]]) = {
    path("""/%s/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])*)/?)?)""".format(prefix)) { 
      new DelegatingService[A, APIKeyRecord => Future[B], A, (APIKeyRecord, Path) => Future[B]] {
        val delegate = next
        val service = (request: HttpRequest[A]) => {
          val path: Option[String] = request.parameters.get('prefixPath).filter(_ != null) 
          next.service(request) map { f => (apiKey: APIKeyRecord) => f(apiKey, Path(path.getOrElse(""))) }
        }

        val metadata = None
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
