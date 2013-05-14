package com.precog.shard.service

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
