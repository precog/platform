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

import blueeyes.json._

import com.precog.common.security._

import java.util.UUID

import org.joda.time.DateTime

import scala.collection.mutable

import scalaz._


final class InMemoryJobManager[M[+_]](implicit val M: Monad[M]) extends JobManager[M] with JobStateManager[M] {
  import scalaz.syntax.monad._
  import JobState._

  val jobs: mutable.Map[JobId, Job] = mutable.Map.empty

  val channels: mutable.Map[(JobId, String), List[Message]] = mutable.Map.empty

  val statuses: mutable.Map[JobId, List[Status]] = mutable.Map.empty

  val status: mutable.Map[JobId, Status] = mutable.Map.empty

  private def newJobId: JobId = UUID.randomUUID().toString.toLowerCase.replace("-", "")

  def createJob(auth: APIKey, name: String, jobType: String, started: Option[DateTime], expires: Option[DateTime]): M[Job] = {
    M.point {
      val state = started map (Started(_, NotStarted)) getOrElse NotStarted
      val job = Job(newJobId, auth, name, jobType, state, expires)
      jobs(job.id) = job
      statuses(job.id) = Nil
      job
    }
  }

  def findJob(id: JobId): M[Option[Job]] = M.point { jobs get id }

  def listJobs(apiKey: APIKey): M[Seq[Job]] = M.point {
    jobs.values.toList filter (_.apiKey == apiKey)
  }

  def updateStatus(jobId: JobId, prevStatus: Option[StatusId], 
      msg: String, progress: BigDecimal, unit: String, extra: Option[JValue]): M[Either[String, Status]] = {

    val jval = JObject(
      JField("message", JString(msg)) ::
      JField("progress", JNum(progress)) ::
      JField("unit", JString(unit)) ::
      (extra map (JField("info", _) :: Nil) getOrElse Nil)
    )

    status get jobId match {
      case Some(curStatus) if curStatus.id == prevStatus.getOrElse(curStatus.id) =>
        for (m <- addMessage(jobId, Message.channels.Status, jval)) yield {
          val Some(s) = Status.fromMessage(m)
          status.put(jobId, s)
          Right(s)
        }

      case Some(_) =>
        M.point(Left("Current status did not match expected status."))

      case None if prevStatus.isDefined =>
        M.point(Left("Job has not yet started, yet a status was expected."))

      case None =>
        for (m <- addMessage(jobId, Message.channels.Status, jval)) yield {
          val Some(s) = Status.fromMessage(m)
          status.put(jobId, s)
          Right(s)
        }
    }
  }

  def getStatus(jobId: JobId): M[Option[Status]] = M.point {
    status get jobId match {
      case Some(status) => Some(status)
      case _ => None
    }
  }

  def listChannels(jobId: JobId): M[Seq[String]] = M.point {
    channels.keys.toList collect {
      case (`jobId`, channel) => channel
    }
  }

  def addMessage(jobId: JobId, channel: String, value: JValue): M[Message] = {
    M.point {
      val posts = channels get (jobId, channel) getOrElse Nil
      val message = Message(jobId, posts.size, channel, value)

      channels((jobId, channel)) = message :: posts
      message
    }
  }

  def listMessages(jobId: JobId, channel: String, since: Option[MessageId]): M[Seq[Message]] = {
    M.point {
      val posts = channels get ((jobId, channel)) getOrElse Nil
      since map { mId => posts.takeWhile(_.id != mId).reverse } getOrElse posts.reverse
    }
  }

  protected def transition(id: JobId)(t: JobState => Either[String, JobState]): M[Either[String, Job]] = {
    M.point {
      jobs get id map { job =>
        t(job.state) match {
          case Right(newState) =>
            val newJob = job.copy(state = newState)
            jobs(id) = newJob
            Right(newJob)

          case Left(error) =>
            Left(error)
        }
      } getOrElse Left("Cannot find job with ID '%s'." format id)
    }
  }
}

