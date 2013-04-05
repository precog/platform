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

class QueryDeleteHandler[A](jobManager: JobManager[Future], clock: Clock)(implicit executor: ExecutionContext, M: Monad[Future])
extends CustomHttpService[A, APIKey => Future[HttpResponse[A]]] {
  import JobState._
  import scalaz.syntax.monad._

  val service = { (request: HttpRequest[A]) =>
    Success({ (apiKey: APIKey) =>
      request.parameters get 'jobId map { jobId =>
        jobManager.cancel(jobId, "User request through HTTP.", clock.now()) map {
          case Left(error) =>
            HttpResponse[A](HttpStatus(BadRequest, error))
          case Right(_) =>
            HttpResponse[A](Accepted)
        }
      } getOrElse {
        Future(HttpResponse[A](HttpStatus(BadRequest, "Missing required 'jobId parameter.")))
      }
    })
  }

  val metadata = DescriptionMetadata("""Requests the deletion of an asynchronous query.""")
}

