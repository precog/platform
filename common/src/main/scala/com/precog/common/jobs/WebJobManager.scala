package com.precog.common
package jobs

import com.precog.common.security._
import com.precog.common.JValueByteChunkTranscoders._

import java.nio.ByteBuffer

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpStatusCodes.{ Response => _, _ }
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import org.joda.time.DateTime

import org.streum.configrity.Configuration

import com.weiglewilczek.slf4s.Logging

import scalaz._

object WebJobManager extends client.BaseClient {
  def apply(config: Configuration)(implicit ec: ExecutionContext): JobManager[Response] = {
    RealWebJobManager(
      config[String]("service.protocol", "http"),
      config[String]("service.host", "localhost"),
      config[Int]("service.port", 80),
      config[String]("service.path", "/jobs/v1/")
    )
  }
}

case class RealWebJobManager(protocol: String, host: String, port: Int, path: String)(implicit val executionContext: ExecutionContext) extends WebJobManager {
  val M = new blueeyes.bkka.FutureMonad(executionContext)
  protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
}

import WebJobManager._

trait WebJobManager extends JobManager[Response] with JobStateManager[Response] with Logging {
  import scalaz.syntax.monad._
  import EitherT.{ left => leftT, right => rightT, _ }
  import \/.{ left, right }
  import DefaultBijections._
  import blueeyes.json.serialization.DefaultSerialization._

  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A

  // This could be JValue, but too many problems arise w/ ambiguous implicits.
  private def withClient[A](f: HttpClient[ByteChunk] => A): A = withRawClient { client =>
    f(client.contentType[ByteChunk](application/MimeTypes.json))
  }

  private def unexpected[A](resp: HttpResponse[A]): String = "Unexpected response from server:\n" + resp

  def createJob(apiKey: APIKey, name: String, jobType: String, started: Option[DateTime], expires: Option[DateTime]): Response[Job] = {
    val content: JValue = jobject(
      jfield("name", name),
      jfield("type", jobType)
    )

    withClient { client =>
      val job0: Response[Job] = eitherT(client.query("apiKey", apiKey).post("/jobs/")(content) map {
        case HttpResponse(HttpStatus(Created, _), _, Some(obj), _) =>
          obj.validated[Job] map (right(_)) getOrElse left("Invalid job returned by server:\n" + obj)
        case res =>
          left(unexpected(res))
      })

      started map { timestamp =>
        for {
          initJob <- job0
          result <- start(initJob.id, timestamp)
          job <- result.fold({ error: String => BadResponse("Server failed to return job that had been created: " + error) }, _.point[Response])
        } yield job
      } getOrElse job0
    }
  }

  def findJob(jobId: JobId): Response[Option[Job]] = {
    withClient { client =>
      eitherT(client.get[JValue]("/jobs/" + jobId) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[Job] map { job => right(Some(job)) } getOrElse left("Invalid job returned from server:\n" + obj)
        case HttpResponse(HttpStatus(NotFound, _), _, _, _) =>
          right(None)
        case res =>
          left(unexpected(res))
      })
    }
  }

  def listJobs(apiKey: APIKey): Response[Seq[Job]] = {
    withClient { client =>
      eitherT(client.query("apiKey", apiKey).get[JValue]("/jobs/") map {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[Vector[Job]] map (right(_)) getOrElse left("Invalid list of jobs returned from server:\n" + obj)
        case res =>
          left(unexpected(res))
      })
    }
  }

  def updateStatus(jobId: JobId, prevStatus: Option[StatusId], msg: String, progress: BigDecimal, unit: String, info: Option[JValue]): Response[Either[String, Status]] = {
    withClient { client0 =>
      val update = JObject(
        JField("message", JString(msg)) ::
        JField("progress", JNum(progress)) ::
        JField("unit", JString(unit)) ::
        (info map (JField("info", _) :: Nil) getOrElse Nil)
      )

      val client = prevStatus map { id => client0.query("prevStatusId", id.toString) } getOrElse client0
      eitherT(client.put[JValue]("/jobs/" + jobId + "/status")(update) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[Message] map (Status.fromMessage(_)) match {
            case Success(Some(status)) => right(Right(status))
            case Success(None) => left("Invalid status message returned from server.")
            case Failure(error) => left("Invalid content returned from server.")
          }
        case HttpResponse(HttpStatus(Conflict, _), _, Some(JString(msg)), _) =>
          right(Left(msg))
        case HttpResponse(HttpStatus(NotFound, _), _, _, _) =>
          right(Left("No job exists with the given job ID: " + jobId))
        case res =>
          left(unexpected(res))
      })
    }
  }

  def getStatus(jobId: JobId): Response[Option[Status]] = withClient { client =>
    eitherT(client.get[JValue]("/jobs/" + jobId + "/status") map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Message] map (Status.fromMessage(_)) match {
          case Success(Some(status)) => right(Some(status))
          case _ => left("Invalid status returned from upstream server:\n" + obj)
        }
      case HttpResponse(HttpStatus(NotFound, _), _, _, _) =>
        right(None)
      case res =>
        left(unexpected(res))
    })
  }

  def listChannels(jobId: JobId): Response[Seq[String]] = withClient { client =>
    eitherT(client.get[JValue]("/jobs/" + jobId + "/messages/") map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Vector[String]] map (right(_)) getOrElse left("Invalid list of channels returned from server:\n" + obj)
      case res =>
        left(unexpected(res))
    })
  }

  def addMessage(jobId: JobId, channel: String, value: JValue): Response[Message] = withClient { client =>
    eitherT(client.post[JValue]("/jobs/" + jobId + "/messages/" + channel)(value) map {
      case HttpResponse(HttpStatus(Created, _), _, Some(obj), _) =>
        obj.validated[Message] map (right(_)) getOrElse left("Invalid message returned from server:\n" + obj)
      case res =>
        left(unexpected(res))
    })
  }

  def listMessages(jobId: JobId, channel: String, since: Option[MessageId]): Response[Seq[Message]] = withClient { client0 =>
    val client = since map { id => client0.query("after", id.toString) } getOrElse client0
    eitherT(client.get[JValue]("/jobs/" + jobId + "/messages/" + channel) map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Vector[Message]] map (right(_)) getOrElse left("Invalid list of messages returned from server:\n" + obj)
      case res =>
        left(unexpected(res))
    })
  }

  protected def transition(jobId: JobId)(t: JobState => Either[String, JobState]): Response[Either[String, Job]] = withClient { client =>
    findJob(jobId) flatMap {
      case Some(job) =>
        t(job.state) match {
          case Right(state) =>
            Response(client.put[JValue]("/jobs/" + jobId + "/state")(state.serialize)) flatMap {
              case HttpResponse(HttpStatus(OK, _), _, _, _) =>
                findJob(jobId) map {
                  case Some(job) => Right(job)
                  case None => Left("Could not find job with ID: " + jobId)
                }
              case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString(msg)), _) =>
                BadResponse(msg)
              case res =>
                BadResponse(unexpected(res))
            }

          case Left(msg) =>
            rightT(Left(msg).point[Future])
        }

      case None =>
        BadResponse("Could not find job with ID: " + jobId)
    }
  }

  private val isoFormat = org.joda.time.format.ISODateTimeFormat.dateTime()

  override def finish(jobId: JobId, result: Option[JobResult], finishedAt: DateTime = new DateTime): Response[Either[String, Job]] = {
    withRawClient { client0 =>
      val client1 = client0.query("timestamp", isoFormat.print(finishedAt))

      val response = Response(result map { case JobResult(mimeTypes, content) =>
        mimeTypes.foldLeft(client1)(_ contentType _)
                 .put[ByteChunk]("/jobs/" + jobId + "/result")(Left(ByteBuffer.wrap(content)))
      } getOrElse {
        client1.put[ByteChunk]("/jobs/" + jobId + "/result")(Right(StreamT.empty))
      })
      response flatMap {
        case HttpResponse(HttpStatus(OK, _), _, _, _) =>
          findJob(jobId) map (_ map (Right(_)) getOrElse {
            Left("Could not find job with ID " + jobId)
          })
        case HttpResponse(HttpStatus(PreconditionFailed, _), _, Some(error), _) =>
          leftT(ByteChunk.forceByteArray(error) map (new String(_, "UTF-8")))
        case res =>
          BadResponse(unexpected(res))
      }
    }
  }
}

