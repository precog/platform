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
package com.precog.heimdall

import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.BijectionsChunkJson._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.util.Clock

import akka.dispatch.{ ExecutionContext, Future }

import com.weiglewilczek.slf4s.Logging

import scalaz._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class CreateJobHandler(jobs: JobManager[Future], clock: Clock)(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    request.content map { contentM =>
      Success(contentM flatMap { content =>
        (content \ "name", content \ "type") match {
          case (JString(name), JString(tpe)) =>
            request.parameters.get('apiKey) map { apiKey =>

              // TODO: Check API Key and make sure it is valid before proceeding.
              // TODO: Remove apiKey from JValue output.

              jobs.createJob(apiKey, name, tpe, Some(clock.now()), None) map { job =>
                HttpResponse[JValue](OK, content = Some(job.serialize))
              }

            } getOrElse {
              Future(HttpResponse[JValue](BadRequest, content = Some("Missing required paramter 'apiKey")))
            }

          case (JUndefined, JUndefined) =>
            Future(HttpResponse[JValue](BadRequest, content = Some("Missing both `name` and `type` of job.")))

          case (name, JUndefined) =>
            Future(HttpResponse[JValue](BadRequest, content = Some("Missing `type` of job.")))

          case (JUndefined, tpe) =>
            Future(HttpResponse[JValue](BadRequest, content = Some("Missing `name` of job.")))

          case (name, tpe) =>
            Future(HttpResponse[JValue](BadRequest, content = Some("Expected `name` and `type` to be strings, but found '%s' and '%s'." format (name, tpe))))
        }
      })
    } getOrElse {
      Failure(DispatchError(BadRequest, "Missing request body (JSON)."))
    }
  }

  val metadata = Some(AboutMetadata(ParameterMetadata('apiKey, None), DescriptionMetadata("Job creation requires an 'apiKey.")))
}

class GetJobHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    request.parameters.get('jobId) map { jobId =>
      Success(jobs.findJob(jobId) map { job =>
        HttpResponse[JValue](OK, content = Some(job.serialize))
      })
    } getOrElse {
      Failure(DispatchError(BadRequest, "Missing 'jobId paramter."))
    }
  }

  val metadata = Some(AboutMetadata(ParameterMetadata('jobId, None), DescriptionMetadata("Retrieve jobs by job ID.")))
}

class GetJobStatusHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    request.parameters.get('jobId) map { jobId =>
      Success(jobs.getStatus(jobId) map (_ map { status =>
        HttpResponse[JValue](OK, content = Some(Status.toMessage(status).serialize))
      } getOrElse {
        HttpResponse[JValue](NotFound, content = Some(JString("No status has been created for this job yet.")))
      }))
    } getOrElse {
      Failure(DispatchError(BadRequest, "Missing 'jobId paramter."))
    }
  }

  val metadata = Some(AboutMetadata(ParameterMetadata('jobId, None), DescriptionMetadata("Get the latest status of a job.")))
}

class UpdateJobStatusHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    (for {
      contentM <- request.content
      jobId <- request.parameters get 'jobId
    } yield {
      Success(contentM flatMap { content =>
        (content \ "message", content \ "progress", content \ "unit") match {
          case (JString(msg), JNum(progress), JString(unit)) =>
            val prevId = request.parameters.get('prevStatusId) match {
              case Some(StatusId(id)) => Right(Some(id))
              case Some(badId) => Left("Invalid status ID '%s'." format badId)
              case None => Right(None)
            }
            val result = prevId.right map { prevId =>
              jobs.updateStatus(jobId, prevId, msg, progress, unit, content \? "info")
            } match {
              case Right(resultM) => resultM
              case Left(error) => Future(Left(error))
            }

            result map {
              case Right(status) =>
                val msg = Status.toMessage(status)
                HttpResponse[JValue](OK, content = Some(msg.serialize))
              case Left(error) =>
                HttpResponse[JValue](BadRequest, content = Some(JString(error)))
            }

          case (_, _, _) =>
            Future(HttpResponse[JValue](BadRequest, content = Some(JString("Status update requires fields 'message', 'progress', 'unit'."))))
        }
      })
    }) getOrElse {
      Failure(DispatchError(BadRequest, "Status updates require both a JSON content body and a 'jobId."))
    }
  }

  val metadata = Some(AboutMetadata(
    AndMetadata(
      ParameterMetadata('jobId, None),
      ParameterMetadata('prevStatusId, None)
    ),
    DescriptionMetadata("Post status updates to a job.")
  ))
}

class AddMessageHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    (for {
      jobId <- request.parameters get 'jobId
      channel <- request.parameters get 'channel
      contentM <- request.content
    } yield {
      Success(contentM flatMap { message =>
        jobs.addMessage(jobId, channel, message) map { message =>
          HttpResponse[JValue](OK, content = Some(message.serialize))
        }
      })
    }) getOrElse {
      Failure(DispatchError(BadRequest, "Messages require a JSON body in the request."))
    }
  }

  val metadata = Some(AboutMetadata(
    AndMetadata(
      ParameterMetadata('jobId, None),
      ParameterMetadata('channel, None)
    ),
    DescriptionMetadata("Post a message to a channel.")
  ))
}

class ListMessagesHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    (for {
      jobId <- request.parameters get 'jobId
      channel <- request.parameters get 'channel
    } yield {
      val prevId = request.parameters get 'after match {
        case Some(MessageId(id)) => Right(Some(id))
        case Some(badId) => Left("Invalid message ID in 'after '%s'." format badId)
        case None => Right(None)
      }

      Success(prevId match {
        case Right(prevId) =>
          jobs.listMessages(jobId, channel, prevId) map { messages =>
            HttpResponse[JValue](OK, content = Some(messages.toList.serialize))
          }
        case Left(error) =>
          Future(HttpResponse[JValue](BadRequest, content = Some(JString(error))))
      })
    }) getOrElse {
      Failure(DispatchError(BadRequest, "Messages require a 'jobId and a 'channel."))
    }
  }

  val metadata = Some(AboutMetadata(
    AndMetadata(
      ParameterMetadata('jobId, None),
      ParameterMetadata('channel, None)
    ),
    DescriptionMetadata("Retrieve a list of messages posted to a channel.")
  ))
}

