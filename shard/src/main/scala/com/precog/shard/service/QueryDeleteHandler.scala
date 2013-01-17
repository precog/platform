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

import akka.dispatch.{ ExecutionContext, Future }

import java.nio.ByteBuffer

import scalaz._

class QueryDeleteHandler(jobManager: JobManager[Future], clock: Clock)(implicit executor: ExecutionContext, M: Monad[Future])
extends CustomHttpService[ByteChunk, APIKey => Future[HttpResponse[ByteChunk]]] {
  import JobState._
  import scalaz.syntax.monad._

  val service = { (request: HttpRequest[ByteChunk]) =>
    Success({ (apiKey: APIKey) =>
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

