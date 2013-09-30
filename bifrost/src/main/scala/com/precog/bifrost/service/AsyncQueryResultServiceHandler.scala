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
package com.precog.bifrost.service

import com.precog.common.jobs._
import com.precog.common.security._

import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.SerializationImplicits._

import akka.dispatch.{ ExecutionContext, Future }

import java.nio.ByteBuffer
import java.nio.charset.Charset

import scalaz._

class AsyncQueryResultServiceHandler(jobManager: JobManager[Future])(implicit executor: ExecutionContext, M: Monad[Future])
    extends CustomHttpService[ByteChunk, APIKey => Future[HttpResponse[ByteChunk]]] {
  import JobManager._
  import JobState._
  import scalaz.syntax.monad._

  val Utf8 = Charset.forName("utf-8")

  val service = { (request: HttpRequest[ByteChunk]) =>
    Success({ (apiKey: APIKey) =>
      request.parameters get 'jobId map { jobId =>
        jobManager.findJob(jobId) flatMap {
          case Some(job) =>
            job.state match {
              case NotStarted | Started(_, _) | Cancelled(_, _, _) =>
                Future(HttpResponse[ByteChunk](Accepted))
              case Finished(_, _) =>
                for {
                  result <- jobManager.getResult(jobId)
                  warnings <- jobManager.listMessages(jobId, channels.Warning, None)
                  errors <- jobManager.listMessages(jobId, channels.Error, None)
                } yield {
                  result.fold({ _ =>
                    HttpResponse[ByteChunk](NotFound)
                  }, { case (mimeType0, data0) =>
                    val mimeType = mimeType0 getOrElse (MimeTypes.application / MimeTypes.json)
                    if (mimeType != (MimeTypes.application / MimeTypes.json)) {
                      HttpResponse[ByteChunk](HttpStatus(InternalServerError, "Incompatible mime-type of query results."))
                    } else {
                      val headers = HttpHeaders.Empty + `Content-Type`(mimeType)
                      val data = data0
                      val prefix = ("""{ "errors": %s, "warnings": %s, "data": """ format (
                        JArray(errors.toList map (_.value)).renderCompact,
                        JArray(warnings.toList map (_.value)).renderCompact
                      )).getBytes(Utf8)
                      val suffix = " }".getBytes(Utf8) :: StreamT.empty[Future, Array[Byte]]

                      val chunks = Right(prefix :: (data ++ suffix))
                      HttpResponse[ByteChunk](OK, headers, Some(chunks))
                    }
                  })
                }
              case Aborted(_, _, _) | Expired(_, _) =>
                Future(HttpResponse[ByteChunk](Gone))
            }

          case None =>
            Future(HttpResponse[ByteChunk](NotFound))
        }
      } getOrElse {
        Future(HttpResponse[ByteChunk](HttpStatus(BadRequest, "Missing required 'jobId parameter.")))
      }
    })
  }

  val metadata = DescriptionMetadata("""Takes a job ID and may return the results of the execution of that query.""")
}
