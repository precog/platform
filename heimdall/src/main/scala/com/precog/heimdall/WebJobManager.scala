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

import blueeyes._
import blueeyes.core.data.{BijectionsChunkJson, BijectionsChunkFutureJson, BijectionsChunkString, ByteChunk}
import blueeyes.core.http._
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

case class WebJobManager(protocol: String, host: String, port: Int, path: String)
    extends JobManager[Future] with JobStateManager[Future] with Logging {
  def withClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb
    f(client.protocol(protocol).host(host).port(port).path(path).contentType(application/MimeTypes.json))
  }

  def createJob(apiKey: APIKey, name: String, jobType: String, started: Option[DateTime], expires: Option[DateTime]): M[Job] = {
    val content = JObject(List(
      JField("name", name),
      JField("type", jobType)
    ))

    withClient { client =>
      client.query("apiKey", apiKey)
            .post[JValue]("jobs/")(content) flatMap {
        case HttpResponse(HttpStatus(Created, _), _, Some(obj), _) =>
          val job = ojb.validated[Job] getOrElse sys.error("TODO: Handle this case.")
          started map (start(job.id, _)) getOrElse Future(job)
        case _ =>
          // TODO: createJob should really return Validation[String, Job].
          sys.error("Failure creating job.")
      }
    }
  }

  def findJob(jobId: JobId): M[Option[Job]] = {
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

  def listJobs(show: JobState => Boolean = (_ => true)): Future[Seq[Job]] = {
    withClient { client =>
      client.get[JValue]("jobs") map {
        case HttpResponse(HttpStatus(OK, _), _, Some(objs), _) =>
          obj.validated[Seq[Job]] getOrElse Seq.empty[Job]
        case _ =>
          Seq.empty[Job]
      }
    }
  }

  def updateStatus(jobId: JobId, prevStatus: Option[StatusId], msg: String, progress: BigDecimal, unit: String, info: Option[JValue]): M[Either[String, Status]] = {
    withClient { client0 =>
      val update = JObject(List(
        JField("message", JString(msg)),
        JField("progress", JNum(progress)),
        JField("unit", JString(unit)),
        JField("info", info getOrElse JUndefined)
      ))

      val client = prevStatus map { id => client0.query("prevStatusId", id) } getOrElse client0
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

  def getStatus(jobId: JobId): M[Option[Status]] = withClient { client =>
    client.get[JValue]("jobs/" + jobId + "/status") map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Message].toOption flatMap (Status.fromMessage(_))
      case _ =>
        None
    }
  }

  def listChannels(jobId: JobId): M[Seq[String]] = withClient { client =>
    client.get[JValue]("/jobs/" + jobId + "/messages") map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Seq[String]] getOrElse Seq.empty[String]
      case _ =>
        Seq.empty[String]
    }
  }

  def addMessage(jobId: JobId, channel: String, value: JValue): M[Message] = withClient { client =>
    client.post[JValue]("jobs/" + jobId + "/messages/" + channel)(value) map {
      case HttpResponse(HttpStatus(Created, _), _, Some(obj), _) =>
        obj.validated[Message] getOrElse sys.error("Invalid message.")
      case _ =>
        sys.error("Unexpected response from server.")
    }
  }

  def listMessages(jobId: JobId, channel: String, since: Option[MessageId]): M[Seq[Message]] = withClient { client0 =>
    val client = since map { id => client0.query("after", id) } getOrElse client0
    client.get[JValue]("jobs/" + jobId + "/messsages/" + channel) map {
      case HttpResponse(HttpStatus(OK, _), _, Some(obj), _) =>
        obj.validated[Seq[Message]] getOrElse sys.error("Invalid messages returned from server.")
      case _ =>
        sys.error("Unexpected response from server.")
    }
  }

  protected def transition(jobId: JobId)(t: JobState => Either[String, JobState]): M[Either[String, Job]] = withClient { client =>
    findJob(jobId) flatMap { job =>
      t(job.state) match {
        case Right(state) =>
          client.post[JValue]("jobs/" + jobId + "/state")(state.serialize) flatMap {
            case HttpResponse(HttpStatus(OK, _), _, _, _) =>
              findJob(jobId) map (Right(_))
            case HttpResponse(HttpStatus(BadRequest, _), _, Some(JString(msg)), _) =>
              Future(Left(msg))
            case _ =>
              Future(Left("Unexpected response from server."))
          }
        case Left(msg) =>
          Future(Left(msg))
      }
    }
  }

  override def finish(job: JobId, result: Option[JobResult], finishedAt: DateTime = new DateTime): M[Either[String, Job]] = sys.error("todo")
}

object WebJobManager {
  def apply(config: Configuration): JobManager[Future] = {
    WebJobManager(
      config[String]("service.protocol", "http"),
      config[String]("service.host", "localhost"),
      config[String]("service.port", 80),
      config[String]("service.path", "/jobs/v1/")
    )
  }
}

