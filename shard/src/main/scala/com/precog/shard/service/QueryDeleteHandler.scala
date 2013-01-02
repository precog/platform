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
package com.precog.shard.service

import com.precog.common.jobs._
import com.precog.common.security._

import blueeyes.util.Clock
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.json._

import akka.dispatch.{ MessageDispatcher, Future }

import java.nio.ByteBuffer

import scalaz._

class QueryDeleteHandler(jobManager: JobManager[Future], clock: Clock)(implicit dispatcher: MessageDispatcher, M: Monad[Future])
extends CustomHttpService[ByteChunk, APIKeyRecord => Future[HttpResponse[ByteChunk]]] {
  import JobState._
  import scalaz.syntax.monad._

  val service = { (request: HttpRequest[ByteChunk]) =>
    Success({ (apiKey: APIKeyRecord) =>
      request.parameters get 'jobId map { jobId =>
        jobManager.cancel(jobId, "User request through HTTP.", clock.now()) map {
          case Left(error) =>
            HttpResponse[ByteChunk](HttpStatus(BadRequest, error))
          case Right(_) =>
            HttpResponse[ByteChunk](Accepted)
        }
      } getOrElse {
        Future(HttpResponse[ByteChunk](HttpStatus(BadRequest, "Missing required 'jobId parameter.")))
      }
    })
  }

  val metadata = Some(DescriptionMetadata(
    """Requests the deletion of an asynchronous query."""
  ))
}

