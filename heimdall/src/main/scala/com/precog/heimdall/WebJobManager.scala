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

import com.precog.common.security._

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpStatusCodes._
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

case class RealWebJobManager(protocol: String, host: String, port: Int, path: String)(implicit val asynContext: ExecutionContext) extends WebJobManager {
  protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
}

trait WebJobManager extends JobManager[Future] with JobStateManager[Future] with BijectionsChunkFutureByteArray with Logging {

  import BijectionsChunkByteArray._
  import BijectionsChunkFutureByteArray._
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  implicit def asynContext: ExecutionContext

  protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A

  private def withClient[A](f: HttpClient[JValue] => A): A = withRawClient { client =>
    f(client.contentType[JValue](application/MimeTypes.json))
  }

  def createJob(apiKey: APIKey, name: String, jobType: String, started: Option[DateTime], expires: Option[DateTime]): Future[Job] = {
    val content: JValue = JObject(List(
      JField("name", name),
      JField("type", jobType)
    ))

    withRawClient { client =>
      val body: JValue = JObject(List(
        JField("name", JString("abc")),
        JField("type", JString("abc"))
      ))

      client.contentType[JValue](MimeTypes.application / MimeTypes.json)
             .query("apiKey", "xxx")
             .post("/jobs")(body) map {
        case HttpResponse(HttpStatus(Created, _), _, _, _) => println("!!!!!")
        case x => println(" =( " + x)
      }
    }

    withClient { client =>
      client.query("apiKey", apiKey)
            .post("/jobs")(content) flatMap {
        case HttpResponse(HttpStatus(Created, _), _, Some(obj), _) =>
          val job = obj.validated[Job] getOrElse sys.error("TODO: Handle this case.")
          started map { timestamp =>
            start(job.id, timestamp) flatMap {
              case Left(error) =>
                findJob(job.id) map (_ getOrElse sys.error("Exepcted to find job but couldn't."))
              case Right(job) =>
                Future(job)
            }
          } getOrElse Future(job)
        case x =>
          sys.error("Failure creating job. " + x)
      }
    }
  }

  def findJob(jobId: JobId): Future[Option[Job]] = {
    withClient { client =>
      client.get[JValue]("jobs/" + jobId) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[Job].toOption
        case HttpResponse(HttpStatus(NotFound, _), _, _, _) =>
          None
        case HttpResponse(HttpStatus(_, _), _, _, _) =>
          logger.warn("Failed to communicate with jobs service.")
          None
      }
    }
  }

  def listJobs(apiKey: APIKey): Future[Seq[Job]] = {
    withClient { client =>
      client.query("apiKey", apiKey).get[JValue]("jobs") map {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[Vector[Job]] getOrElse Vector.empty[Job]
        case _ =>
          Vector.empty[Job]
      }
    }
  }

  def updateStatus(jobId: JobId, prevStatus: Option[StatusId], msg: String, progress: BigDecimal, unit: String, info: Option[JValue]): Future[Either[String, Status]] = {
    withClient { client0 =>
      val update = JObject(List(
        JField("message", JString(msg)),
        JField("progress", JNum(progress)),
        JField("unit", JString(unit)),
        JField("info", info getOrElse JUndefined)
      ))

      val client = prevStatus map { id => client0.query("prevStatusId", id.toString) } getOrElse client0
      client.put[JValue]("jobs/" + jobId + "/status")(update) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
          obj.validated[Message] map (Status.fromMessage(_)) match {
            case Success(Some(status)) => Right(status)
            case Success(None) => Left("Invalid status message returned from server.")
            case Failure(error) => Left("Invalid content returned from server.")
          }
        case HttpResponse(HttpStatus(NotFound, _), _, _, _) =>
          Left("Not job exists with the given job ID: " + jobId)
        case _ =>
          Left("Unexpected response from server.")
      }
    }
  }

  def getStatus(jobId: JobId): Future[Option[Status]] = withClient { client =>
    client.get[JValue]("jobs/" + jobId + "/status") map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Message].toOption flatMap (Status.fromMessage(_))
      case _ =>
        None
    }
  }

  def listChannels(jobId: JobId): Future[Seq[String]] = withClient { client =>
    client.get[JValue]("/jobs/" + jobId + "/messages") map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Vector[String]] getOrElse Nil
      case _ =>
        Seq.empty[String]
    }
  }

  def addMessage(jobId: JobId, channel: String, value: JValue): Future[Message] = withClient { client =>
    client.post[JValue]("jobs/" + jobId + "/messages/" + channel)(value) map {
      case HttpResponse(HttpStatus(Created, _), _, Some(obj), _) =>
        obj.validated[Message] getOrElse sys.error("Invalid message.")
      case _ =>
        sys.error("Unexpected response from server.")
    }
  }

  def listMessages(jobId: JobId, channel: String, since: Option[MessageId]): Future[Seq[Message]] = withClient { client0 =>
    val client = since map { id => client0.query("after", id.toString) } getOrElse client0
    client.get[JValue]("jobs/" + jobId + "/messsages/" + channel) map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Vector[Message]] getOrElse sys.error("Invalid messages returned from server.")
      case _ =>
        sys.error("Unexpected response from server.")
    }
  }

  protected def transition(jobId: JobId)(t: JobState => Either[String, JobState]): Future[Either[String, Job]] = withClient { client =>
    findJob(jobId) flatMap {
      case Some(job) =>
        t(job.state) match {
          case Right(state) =>
            client.post[JValue]("jobs/" + jobId + "/state")(state.serialize) flatMap {
              case HttpResponse(HttpStatus(OK, _), _, _, _) =>
                findJob(jobId) map {
                  case Some(job) => Right(job)
                  case None => Left("Could not find job with ID: " + jobId)
                }
              case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString(msg)), _) =>
                Future(Left(msg))
              case _ =>
                Future(Left("Unexpected response from server."))
            }
          case Left(msg) =>
            Future(Left(msg))
        }

      case None =>
        Future(Left("Could not find job with ID: " + jobId))
    }
  }

  private val isoFormat = org.joda.time.format.ISODateTimeFormat.dateTime()

  override def finish(jobId: JobId, result: Option[JobResult], finishedAt: DateTime = new DateTime): Future[Either[String, Job]] = {
    withRawClient { client0 =>
      val client1 = client0.query("timestamp", isoFormat.print(finishedAt))

      val response = result map { case JobResult(mimeTypes, content) =>
        mimeTypes.foldLeft(client1)(_ contentType _)
                 .post[Array[Byte]]("/jobs/" + jobId + "/result")(content)
      } getOrElse {
        client1.post[Array[Byte]]("/jobs/" + jobId + "/result")(new Array[Byte](0))
      }
      response flatMap {
        case HttpResponse(HttpStatus(OK, _), _, _, _) =>
          findJob(jobId) map (_ map (Right(_)) getOrElse {
            Left("Could not find job with ID " + jobId)
          })
        case HttpResponse(HttpStatus(PreconditionFailed, _), _, Some(error), _) =>
          Future(Left(new String(error, "UTF-8")))
        case _ =>
          Future(Left("Unexpected response from server."))
      }
    }
  }
}

object WebJobManager {
  def apply(config: Configuration)(implicit ec: ExecutionContext): JobManager[Future] = {
    RealWebJobManager(
      config[String]("service.protocol", "http"),
      config[String]("service.host", "localhost"),
      config[Int]("service.port", 80),
      config[String]("service.path", "/jobs/v1/")
    )
  }

  type Response[+A] = EitherT[Future, String, A]
}

