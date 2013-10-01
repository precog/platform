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
package com.precog.dvergr

import com.precog.common.jobs._
import com.precog.common.security._

import java.util.UUID

import akka.dispatch._
import akka.util.Timeout

import org.joda.time.DateTime

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._
import blueeyes.persistence.mongo.dsl._
import blueeyes.json.serialization.Extractor
import blueeyes.json.serialization.DefaultSerialization._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz._
import scalaz.std.option._


trait ManagedMongoJobManagerModule {
  implicit def executionContext: ExecutionContext
  implicit def M: Monad[Future]

  def jobManager(config: Configuration): (Mongo, JobManager[Future]) = {
    import MongoJobManagerSettings.default

    val mongo = RealMongo(config.detach("mongo"))

    val database = config[String]("mongo.database", "jobs_v1")

    val timeout = config[Long]("mongo.timeout", default.queryTimeout.duration.toMillis)
    val jobs = config[String]("mongo.jobsCollection", default.jobs)
    val messages = config[String]("mongo.messagesCollection", default.messages)

    val settings = MongoJobManagerSettings(timeout, jobs, messages)

    val fs = GridFSFileStorage[Future](config.detach("mongo"))

    (mongo, new MongoJobManager(mongo.database(database), settings, fs))
  }
}

case class MongoJobManagerSettings(queryTimeout: Timeout, jobs: String, messages: String)
object MongoJobManagerSettings {
  val default: MongoJobManagerSettings = MongoJobManagerSettings(5000, "jobs", "jog_messages")
}

final class MongoJobManager(database: Database, settings: MongoJobManagerSettings, fs0: FileStorage[Future])
    (implicit executionContext: ExecutionContext)
    extends JobManager[Future] with JobStateManager[Future] with JobResultManager[Future] with Logging {

  import JobManager._
  import JobState._

  protected val fs = fs0
  implicit val M = new FutureMonad(executionContext)
  implicit val queryTimeout = settings.queryTimeout

  // Ensure proper indices for job/message lookup
  database(ensureIndex("jobs_index").on(".id").in(settings.jobs))
  database(ensureIndex("apikey_index").on(".apiKey").in(settings.jobs))

  database(ensureIndex("jobid_index").on(".jobId").in(settings.messages))

  private def newJobId(): String = UUID.randomUUID().toString.toLowerCase.replace("-", "")

  def createJob(apiKey: APIKey, name: String, jobType: String, data: Option[JValue], started: Option[DateTime]): Future[Job] = {
    val start = System.nanoTime
    val id = newJobId()
    val state = started map (Started(_, NotStarted)) getOrElse NotStarted
    val job = Job(id, apiKey, name, jobType, data, state)
    database(insert(job.serialize.asInstanceOf[JObject]).into(settings.jobs)) map { _ =>
      logger.info("Job %s created in %f ms".format(id, (System.nanoTime - start) / 1000000.0))
      job
    }
  }

  def findJob(jobId: JobId): Future[Option[Job]] = {
    database(selectOne().from(settings.jobs).where("id" === jobId)) map {
      _ map (_.deserialize[Job])
    }
  }

  def listJobs(apiKey: APIKey): Future[Seq[Job]] = {
    database(selectAll.from(settings.jobs).where("apiKey" === apiKey)) map {
      _.map(_.deserialize[Job]).toList
    }
  }

  def updateStatus(jobId: JobId, prevStatusId: Option[StatusId],
    msg: String, progress: BigDecimal, unit: String, extra: Option[JValue]): Future[Either[String, Status]] = {

    val start = System.nanoTime
    nextMessageId(jobId) flatMap { statusId =>
      prevStatusId match {
        case Some(prevId) =>
          database(selectAndUpdate(settings.jobs)
              .set(JPath("status") set statusId)
              .where("id" === jobId && "status" === prevId)) flatMap {
            case Some(_) => // Success
              val status = Status(jobId, statusId, msg, progress, unit, extra)
              val message = Status.toMessage(status)
              database(insert(message.serialize.asInstanceOf[JObject]).into(settings.messages)) map { _ =>
                logger.trace("Job %s updated in %f ms".format(jobId, (System.nanoTime - start) / 1000.0))
                Right(status)
              }

            case None => // Failed
              Future { Left("Current status ID didn't match that given: " + prevId) }
          }

        case None =>
          val status = Status(jobId, statusId, msg, progress, unit, extra)
          val message = Status.toMessage(status)
          database {
            upsert(settings.jobs).set(JPath("status") set statusId).where("id" === jobId)
          } flatMap { _ =>
            database(insert(message.serialize.asInstanceOf[JObject]).into(settings.messages))
          } map { _ =>
            Right(status)
          }
      }
    }
  }

  def getStatus(jobId: JobId): Future[Option[Status]] = {

    // TODO: Get Job object, find current status ID, then use that as since.
    // It'll include at least the last status, but rarely much more.

    listMessages(jobId, channels.Status, None) map (_.lastOption flatMap (Status.fromMessage(_)))
  }

  private def nextMessageId(jobId: JobId): Future[Long] = {
    database(selectAndUpsert(settings.jobs)
        .set(JPath("sequence") inc 1)
        .where("id" === jobId)
        .returnNew(true)) map {
      case Some(obj) =>
        (obj \ "sequence").validated[Long] getOrElse sys.error("Expected an integral sequence number.")
      case None =>
        sys.error("Sequence number doesn't exist. This shouldn't happen.")
    }
  }

  def listChannels(jobId: JobId): Future[Seq[String]] = {
    database {
      distinct("channel").from(settings.messages).where("jobId" === jobId)
    } map (_.collect {
      case JString(channel) => channel
    }.toList)
  }

  def addMessage(jobId: JobId, channel: String, value: JValue): Future[Message] = {
    nextMessageId(jobId) flatMap { id =>
      val message = Message(jobId, id, channel, value)
      database {
        insert(message.serialize.asInstanceOf[JObject]).into(settings.messages)
      } map { _ =>
        message
      }
    }
  }

  def listMessages(jobId: JobId, channel: String, since: Option[MessageId]): Future[Seq[Message]] = {
    val filter0 = "jobId" === jobId && "channel" === channel
    val filter = since map { id => filter0 && MongoFieldFilter("id", MongoFilterOperators.$gt, id) } getOrElse filter0
    database {
      selectAll.from(settings.messages).where(filter)
    } map { _.map(_.deserialize[Message]).toList }
  }

  protected def transition(jobId: JobId)(t: JobState => Either[String, JobState]): Future[Either[String, Job]] = {
    findJob(jobId) flatMap {
      case Some(job) =>
        t(job.state) match {
          case Right(newState) =>
            val newJob = job.copy(state = newState)
            database {
              update(settings.jobs).set(newJob.serialize.asInstanceOf[JObject]).where("id" === job.id)
            } map { _ =>
              Right(newJob)
            }

          case Left(error) =>
            Future { Left(error) }
        }

      case None =>
        Future { Left("Cannot find job with ID '%s'." format jobId) }
    }
  }
}
