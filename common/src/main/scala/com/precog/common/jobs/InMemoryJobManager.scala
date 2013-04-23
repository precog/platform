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

import blueeyes.json._
import blueeyes.core.http.MimeType

import com.precog.common.security._
import com.precog.util.cache._

import akka.util.Duration

import java.util.UUID

import org.joda.time.DateTime

import scala.collection.mutable

import org.streum.configrity.Configuration

import scalaz._

private[jobs] case class JobData(job: Job, channels: Map[String, List[Message]], status: Option[Status])

final class InMemoryJobManager[M[+_]](implicit val M: Monad[M]) extends BaseInMemoryJobManager[M] {
  private[jobs] val jobs: mutable.Map[JobId, JobData] =
    new mutable.HashMap[JobId, JobData] with mutable.SynchronizedMap[JobId, JobData]
}

final class ExpiringJobManager[M[+_]](timeout: Duration)(implicit val M: Monad[M]) extends BaseInMemoryJobManager[M] {
  private[jobs] val jobs: mutable.Map[JobId, JobData] = Cache.simple(Cache.ExpireAfterAccess(timeout))
}

object ExpiringJobManager {
  def apply[M[+_]: Monad](config: Configuration): ExpiringJobManager[M] = {
    val timeout = Duration(config[Int]("service.timeout", 900), "seconds")
    new ExpiringJobManager[M](timeout)
  }
}

trait BaseInMemoryJobManager[M[+_]] extends JobManager[M]
    with JobStateManager[M] with JobResultManager[M] {

  import scalaz.syntax.monad._
  import JobState._

  implicit def M: Monad[M]

  private[jobs] val jobs: mutable.Map[JobId, JobData]

  val fs = new InMemoryFileStorage[M]

  private def newJobId: JobId = UUID.randomUUID().toString.toLowerCase.replace("-", "")

  def createJob(auth: APIKey, name: String, jobType: String, data: Option[JValue], started: Option[DateTime]): M[Job] = {
    M.point {
      val state = started map (Started(_, NotStarted)) getOrElse NotStarted
      val job = Job(newJobId, auth, name, jobType, data, state)
      jobs(job.id) = JobData(job, Map.empty, None)
      job
    }
  }

  def findJob(id: JobId): M[Option[Job]] = M.point { jobs get id map (_.job) }

  def listJobs(apiKey: APIKey): M[Seq[Job]] = M.point {
    jobs.values.toList map (_.job) filter (_.apiKey == apiKey)
  }

  def updateStatus(jobId: JobId, prev: Option[StatusId], 
      msg: String, progress: BigDecimal, unit: String, extra: Option[JValue]): M[Either[String, Status]] = {

    val jval = JObject(
      JField("message", JString(msg)) ::
      JField("progress", JNum(progress)) ::
      JField("unit", JString(unit)) ::
      (extra map (JField("info", _) :: Nil) getOrElse Nil)
    )

    synchronized {
      jobs get jobId map {
        case JobData(_, _, Some(cur)) if cur.id == prev.getOrElse(cur.id) =>
          for (m <- addMessage(jobId, JobManager.channels.Status, jval)) yield {
            val Some(s) = Status.fromMessage(m)
            jobs(jobId) = jobs(jobId).copy(status = Some(s))
            Right(s)
          }

        case JobData(_, _, Some(_)) =>
          M.point(Left("Current status did not match expected status."))

        case JobData(_, _, None) if prev.isDefined =>
          M.point(Left("Job has not yet started, yet a status was expected."))

        case JobData(_, _, None) =>
          for (m <- addMessage(jobId, JobManager.channels.Status, jval)) yield {
            val Some(s) = Status.fromMessage(m)
            jobs(jobId) = jobs(jobId).copy(status = Some(s))
            Right(s)
          }
      } getOrElse {
        M.point(Left("No job with ID " + jobId))
      }
    }
  }

  def getStatus(jobId: JobId): M[Option[Status]] = M.point {
    jobs get jobId flatMap (_.status)
  }

  def listChannels(jobId: JobId): M[Seq[String]] = M.point {
    jobs get jobId map (_.channels.keys.toList) getOrElse Nil
  }

  def addMessage(jobId: JobId, channel: String, value: JValue): M[Message] = {
    M.point {
      synchronized {
        val data = jobs(jobId)
        val posts = data.channels.getOrElse(channel, Nil)
        val message = Message(jobId, posts.size, channel, value)
        jobs(jobId) = data.copy(channels = data.channels + (channel -> (message :: posts)))
        message
      }
    }
  }

  def listMessages(jobId: JobId, channel: String, since: Option[MessageId]): M[Seq[Message]] = {
    M.point {
      val posts = jobs(jobId).channels.getOrElse(channel, Nil)
      since map { mId => posts.takeWhile(_.id != mId).reverse } getOrElse posts.reverse
    }
  }

  protected def transition(id: JobId)(t: JobState => Either[String, JobState]): M[Either[String, Job]] = {
    M.point {
      synchronized {
        jobs get id map { case data @ JobData(job, _, _) =>
          t(job.state) match {
            case Right(newState) =>
              val newJob = job.copy(state = newState)
              jobs(id) = data.copy(job = newJob)
              Right(newJob)

            case Left(error) =>
              Left(error)
          }
        } getOrElse Left("Cannot find job with ID '%s'." format id)
      }
    }
  }
}
