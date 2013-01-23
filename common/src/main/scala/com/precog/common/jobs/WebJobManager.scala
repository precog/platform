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
package com.precog.common
package jobs

import com.precog.util.flipBytes
import com.precog.common.security._
import com.precog.common.JValueByteChunkTranscoders._

import java.nio.ByteBuffer

import blueeyes._
import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpHeaders._
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

object WebJobManager {
  def apply(config: Configuration)(implicit ec: ExecutionContext): JobManager[Response] = {
    RealWebJobManager(
      config[String]("service.protocol", "http"),
      config[String]("service.host", "localhost"),
      config[Int]("service.port", 80),
      config[String]("service.path", "/jobs/v1/")
    )
  }

  /**
   * A WebJobManager's monad is, essentially, a future. However, there is a
   * chance that the server could be doing wonky things, so we create a side
   * channel, the left side of the EitherT, for reporting errors related
   * strictly to unexpected communication issues with the server.
   */
  type Response[+A] = EitherT[Future, String, A]

  def Response[A](a: Future[A])(implicit F: Functor[Future]): Response[A] = EitherT.right(a)
  def BadResponse(msg: String)(implicit F: Pointed[Future]): Response[Nothing] = EitherT.left(F.point(msg))

  case class WebJobManagerException(message: String) extends Exception(message)

  /**
   * A natural transformation from Response to Future that maps the left side
   * to exceptions thrown inside the future.
   */
  implicit def ResponseAsFuture(implicit F: Functor[Future]) = new (Response ~> Future) {
    def apply[A](res: Response[A]): Future[A] = res.fold({ error =>
      throw WebJobManagerException(error)
    }, identity)
  }

  implicit def FutureAsResponse(implicit F: Pointed[Future]) = new (Future ~> Response) {
    def apply[A](fa: Future[A]): Response[A] = EitherT.eitherT(fa.map(\/.right).recoverWith {
      case WebJobManagerException(msg) => F.point(\/.left[String, A](msg))
    })
  }

  implicit def FutureStreamAsResponseStream(implicit M: Monad[Future]) = implicitly[Hoist[StreamT]].hoist(FutureAsResponse)
  implicit def ResponseStreamAsFutureStream(implicit M: Monad[Response], F: Functor[Future]) = implicitly[Hoist[StreamT]].hoist(ResponseAsFuture)
}

case class RealWebJobManager(protocol: String, host: String, port: Int, path: String)(implicit val executionContext: ExecutionContext) extends WebJobManager {
  val client = (new HttpClientXLightWeb).protocol(protocol).host(host).port(port).path(path)
  protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A = {
    f(client)
  }
}

import WebJobManager._

trait WebJobManager extends JobManager[Response] with JobStateManager[Response] with Logging {
  import scalaz.syntax.monad._
  import EitherT.{ left => leftT, right => rightT, _ }
  import \/.{ left, right }
  import AkkaTypeClasses._
  import DefaultBijections._
  import blueeyes.json.serialization.DefaultSerialization._

  implicit def executionContext: ExecutionContext

  protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A

  // This could be JValue, but too many problems arise w/ ambiguous implicits.
  private def withClient[A](f: HttpClient[ByteChunk] => A): A = withRawClient { client =>
    f(client.contentType[ByteChunk](application/MimeTypes.json))
  }

  private def unexpected[A](resp: HttpResponse[A]): String = "Unexpected response from server:\n" + resp

  def createJob(apiKey: APIKey, name: String, jobType: String, data: Option[JValue], started: Option[DateTime]): Response[Job] = {
    val content: JValue = JObject(
      JField("name", name) ::
      JField("type", jobType) ::
      (data map (JField("data", _) :: Nil) getOrElse Nil)
    )

    withClient { client =>
      val job0: Response[Job] = eitherT(client.query("apiKey", apiKey).post("")(content) map {
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
      eitherT(client.get[JValue](jobId) map {
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
      eitherT(client.query("apiKey", apiKey).get[JValue]("") map {
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
      eitherT(client.put[JValue](jobId + "/status")(update) map {
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
    eitherT(client.get[JValue](jobId + "/status") map {
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
    eitherT(client.get[JValue](jobId + "/messages/") map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Vector[String]] map (right(_)) getOrElse left("Invalid list of channels returned from server:\n" + obj)
      case res =>
        left(unexpected(res))
    })
  }

  def addMessage(jobId: JobId, channel: String, value: JValue): Response[Message] = withClient { client =>
    eitherT(client.post[JValue](jobId + "/messages/" + channel)(value) map {
      case HttpResponse(HttpStatus(Created, _), _, Some(obj), _) =>
        obj.validated[Message] map (right(_)) getOrElse left("Invalid message returned from server:\n" + obj)
      case res =>
        left(unexpected(res))
    })
  }

  def listMessages(jobId: JobId, channel: String, since: Option[MessageId]): Response[Seq[Message]] = withClient { client0 =>
    val client = since map { id => client0.query("after", id.toString) } getOrElse client0
    eitherT(client.get[JValue](jobId + "/messages/" + channel) map {
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
            Response(client.put[JValue](jobId + "/state")(state.serialize)) flatMap {
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

  def setResult(jobId: JobId, mimeType: Option[MimeType], data: StreamT[Response, Array[Byte]]): Response[Either[String, Unit]] = {
    withRawClient { client0 =>
      eitherT(mimeType.foldLeft(client0)(_ contentType _)
               .put[ByteChunk](jobId + "/result") {
        val t = ResponseStreamAsFutureStream
        Right(t(data) map (ByteBuffer.wrap(_)))
      } map {
        case HttpResponse(HttpStatus(OK, _), _, _, _) => right(Right(()))
        case HttpResponse(HttpStatus(NotFound, _), _, _, _) => right(Left("Cannot find job with id: " + jobId))
        case res => left(unexpected(res))
      })
    }
  }

  def getResult(jobId: JobId): Response[Either[String, (Option[MimeType], StreamT[Response, Array[Byte]])]] = {
    def contentType(headers: HttpHeaders): Option[MimeType] = headers.header[`Content-Type`] flatMap (_.mimeTypes.headOption)

    withRawClient { client =>
      eitherT(client.get[ByteChunk](jobId + "/result") map {
        case HttpResponse(HttpStatus(OK, _), headers, Some(Left(buffer)), _) =>
          right(Right((contentType(headers), flipBytes(buffer) :: StreamT.empty[Response, Array[Byte]])))

        case HttpResponse(HttpStatus(OK, _), headers, Some(Right(chunks)), _) =>
          val t = FutureStreamAsResponseStream
          right(Right((contentType(headers), t(chunks map (flipBytes(_))))))

        case HttpResponse(HttpStatus(NoContent, _), _, _, _) =>
          right(Left("No result has been set yet."))

        case HttpResponse(HttpStatus(NotFound, _), _, _, _) =>
          right(Left("Cannot find job with id: " + jobId))

        case res =>
          left(unexpected(res))
      })
    }
  }
}
