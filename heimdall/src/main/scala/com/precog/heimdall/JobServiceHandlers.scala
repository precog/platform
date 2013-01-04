package com.precog.heimdall

import com.precog.common.jobs._
import com.precog.common.security._

import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.DefaultBijections._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.util.Clock

import akka.dispatch.{ ExecutionContext, Future }

import com.weiglewilczek.slf4s.Logging

import scalaz._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

class ListJobsHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success(request.parameters get 'apiKey map { apiKey =>
      jobs.listJobs(apiKey) map { jobs =>
        HttpResponse(OK, content = Some(JArray((jobs map (_.serialize)).toList)))
      }
    } getOrElse {
      Future(HttpResponse(BadRequest, content = Some(JString("Missing required parameter 'apiKey"))))
    })
  }

  val metadata = Some(AboutMetadata(ParameterMetadata('apiKey, None), DescriptionMetadata("Job creation requires an 'apiKey.")))
}

class CreateJobHandler(jobs: JobManager[Future], auth: AuthService[Future], clock: Clock)(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    request.content map { contentM =>
      Success(contentM flatMap { content =>
        (content \ "name", content \ "type", content \? "data") match {
          case (JString(name), JString(tpe), data) =>
            request.parameters.get('apiKey) map { apiKey =>
              auth.isValid(apiKey) flatMap {
                case true =>
                  jobs.createJob(apiKey, name, tpe, data, None) map { job =>
                    HttpResponse[JValue](Created, content = Some(job.serialize))
                  }
                case false =>
                  Future(HttpResponse[JValue](Forbidden))
              }

            } getOrElse {
              Future(HttpResponse[JValue](BadRequest, content = Some("Missing required paramter 'apiKey")))
            }

          case (JUndefined, JUndefined, _) =>
            Future(HttpResponse[JValue](BadRequest, content = Some("Missing both `name` and `type` of job.")))

          case (name, JUndefined, _) =>
            Future(HttpResponse[JValue](BadRequest, content = Some("Missing `type` of job.")))

          case (JUndefined, tpe, _) =>
            Future(HttpResponse[JValue](BadRequest, content = Some("Missing `name` of job.")))

          case (name, tpe, _) =>
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
      Success(jobs.findJob(jobId) map {
        case Some(job) =>
          HttpResponse[JValue](OK, content = Some(job.serialize))
        case None =>
          HttpResponse[JValue](NotFound)
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
                HttpResponse[JValue](Conflict, content = Some(JString(error)))
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

class ListChannelsHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success(request.parameters get 'jobId map { jobId =>
      jobs.listChannels(jobId) map { channels =>
        HttpResponse(OK, content = Some(channels.serialize))
      }
    } getOrElse {
      Future(HttpResponse(BadRequest, content = Some(JString("Missing required paramter 'jobId"))))
    })
  }

  val metadata = Some(AboutMetadata(
    ParameterMetadata('jobId, None),
    DescriptionMetadata("Return channels that have messages posted to them.")
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
          HttpResponse[JValue](Created, content = Some(message.serialize))
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

class GetJobStateHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success(request.parameters get 'jobId map { jobId =>
      jobs.findJob(jobId) map {
        case Some(job) =>
          HttpResponse[JValue](OK, content = Some(job.state.serialize))
        case None =>
          HttpResponse[JValue](NotFound, content = Some(JString("No job found with id " + jobId)))
      }
    } getOrElse {
      Future(HttpResponse[JValue](BadRequest, content = Some(JString("Missing required 'jobId"))))
    })
  }

  val metadata = Some(AboutMetadata(
    ParameterMetadata('jobId, None),
    DescriptionMetadata("Retreive the current state of a job.")
  ))
}

class PutJobStateHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[JValue], Future[HttpResponse[JValue]]] with Logging {

  def transition(obj: JValue)(f: (DateTime, Option[String]) => Future[Validation[String, JobState]]) = {
    import scalaz.syntax.traverse._
    import scalaz.std.option._

    val result = for {
      timestamp <- (obj \? "timestamp").map(_.validated[DateTime])
                     .sequence[({ type λ[α] = Validation[Error, α] })#λ, DateTime]
      reason <- (obj \? "reason").map(_.validated[String])
                  .sequence[({ type λ[α] = Validation[Error, α] })#λ, String]
    } yield (timestamp getOrElse (new DateTime), reason)

    result match {
      case Success((timestamp, reason)) =>
        f(timestamp, reason) map {
          case Success(jobState) =>
            HttpResponse[JValue](OK, content = Some(jobState.serialize))
          case Failure(error) =>
            HttpResponse[JValue](BadRequest, content = Some(JString(error)))
        }
      case Failure(error) =>
        Future(HttpResponse[JValue](BadRequest, content = Some(JString(error.toString))))
    }
  }

  val service: HttpRequest[Future[JValue]] => Validation[NotServed, Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => {
    Success((for {
      jobId <- request.parameters get 'jobId
      contentM <- request.content
    } yield {
      contentM flatMap { obj =>
        (obj \ "state") match {
          case JString("started") =>
            transition(obj) { (timestamp, _) =>
              jobs.start(jobId, timestamp) map (Validation.fromEither(_)) map (_ map (_.state))
            }

          case JString("cancelled") =>
            transition(obj) {
              case (timestamp, Some(reason)) =>
                jobs.cancel(jobId, reason, timestamp) map (Validation.fromEither(_)) map (_ map (_.state))
              case (_, _) =>
                Future(Failure("Missing required field 'reason' in request body."))
            }

          case JString("aborted") =>
            transition(obj) {
              case (timestamp, Some(reason)) =>
                jobs.abort(jobId, reason, timestamp) map (Validation.fromEither(_)) map (_ map (_.state))
              case (_, _) =>
                Future(Failure("Missing required field 'reason' in request body."))
            }

          case JString("expired") =>
            transition(obj) { (timestamp, _) =>
              jobs.expire(jobId, timestamp) map (Validation.fromEither(_)) map (_ map (_.state))
            }

          case _ =>
            Future(HttpResponse[JValue](BadRequest, content = Some(JString(
              "Invalid 'state given. Expected one of 'start', 'cancel', or 'abort'."
            ))))
        }
      }
    }) getOrElse {
      Future(HttpResponse[JValue](BadRequest, content = Some(JString("Both 'jobId parameter and JSON request body are required."))))
    })
  }

  val metadata = Some(AboutMetadata(
    ParameterMetadata('jobId, None),
    DescriptionMetadata("Update the state of a job if it is permissable.")
  ))
}

class CreateResultHandler(jobs: JobManager[Future], clock: Clock)(implicit ctx: ExecutionContext)
extends CustomHttpService[ByteChunk, Future[HttpResponse[ByteChunk]]] {
  import scalaz.std.option._
  import scalaz.syntax.traverse._

  val service: HttpRequest[ByteChunk] => Validation[NotServed, Future[HttpResponse[ByteChunk]]] = (request: HttpRequest[ByteChunk]) => {
    Success(request.parameters get 'jobId map { jobId =>
      request.content.map(ByteChunk.forceByteArray(_)).sequence[Future, Array[Byte]] flatMap { bytes =>
        val timestamp = request.parameters get 'timestamp flatMap { ts =>
          JString(ts).validated[DateTime].toOption
        } getOrElse clock.now()

        val result = bytes map (JobResult(request.mimeTypes, _))

        jobs.finish(jobId, result, timestamp) map {
          case Right(job) =>
            HttpResponse[ByteChunk](OK)
          case Left(error) =>
            HttpResponse[ByteChunk](PreconditionFailed, content = Some(ByteChunk(error.getBytes("UTF-8"))))
        }
      }
    } getOrElse {
      Future(HttpResponse[ByteChunk](BadRequest,
        content = Some(ByteChunk("Missing required 'jobId parameter.".getBytes("UTF-8")))))
    })
  }

  val metadata = Some(AboutMetadata(
    ParameterMetadata('jobId, None),
    DescriptionMetadata("Put the final result of a finished job.")
  ))
}

class GetResultHandler(jobs: JobManager[Future])(implicit ctx: ExecutionContext)
extends CustomHttpService[Future[Array[Byte]], Future[HttpResponse[Array[Byte]]]] {
  import JobState._

  val service: HttpRequest[Future[Array[Byte]]] => Validation[NotServed, Future[HttpResponse[Array[Byte]]]] = (request: HttpRequest[Future[Array[Byte]]]) => {
    Success(request.parameters get 'jobId map { jobId =>
      jobs.findJob(jobId) map {
        case Some(Job(_, _, _, _, _, Finished(result, _, _))) =>
          HttpResponse[Array[Byte]](OK, content = result map (_.content))
        case _ =>
          HttpResponse[Array[Byte]](NotFound)
      }
    } getOrElse {
      Future(HttpResponse[Array[Byte]](BadRequest))
    })
  }

  val metadata = Some(AboutMetadata(
    ParameterMetadata('jobId, None),
    DescriptionMetadata("Get the results of a job.")
  ))
}
