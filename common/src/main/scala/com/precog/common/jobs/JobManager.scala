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

import org.joda.time.DateTime

import scalaz._

object JobManager {
  object channels {
    val Status = "status"
    val Error = "error"
    val Warning = "warning"
    val Info = "info"
    val Log = "log"
    val ServerError = "serverError"
    val ServerWarning = "serverWarning"
  }
}

trait JobManager[M[+_]] { self =>
  import Message._

  /**
   * Create a new Job with the given API key, name, type and possibly an
   * initial status message and expiration. If a started time is provided, then
   * the job will be put in the Started state, otherwise it will be in the
   * NotStarted state until `start(...)` is run.
   */
  def createJob(auth: APIKey, name: String, jobType: String, data: Option[JValue], started: Option[DateTime]): M[Job]

  /**
   * Returns the Job with the given ID if it exists.
   */
  def findJob(job: JobId): M[Option[Job]]

  /**
   * Returns a list of all currently running jobs
   */
  def listJobs(apiKey: APIKey): M[Seq[Job]]

  /**
   * Updates a job's status to `value`. If a `prevStatus` is provided, then
   * this must match the current status in order for the update to succeed,
   * otherwise the update fails and the actual current status is returned.
   */
  def updateStatus(job: JobId, prevStatus: Option[StatusId], msg: String, progress: BigDecimal, unit: String, extra: Option[JValue]): M[Either[String, Status]]

  /**
   * Returns just the latest status message.
   */
  def getStatus(job: JobId): M[Option[Status]]

  /**
   * Lists all channels that have had messages posted to them. Note that
   * channels are created on a demand by `addMessage`, so this is not a
   * definitive list of channels, just what existed at the time.
   */
  def listChannels(job: JobId): M[Seq[ChannelId]]

  /**
   * Add a message to a job's channel. If the channel does not exist, it will
   * be created.
   */
  def addMessage(job: JobId, channel: ChannelId, value: JValue): M[Message]

  /**
   * Returns all the messages posted to a job's channel since some specified
   * message. If no previous message is given, then all messages posted to the
   * given channel are returned.
   */
  def listMessages(job: JobId, channel: ChannelId, since: Option[MessageId]): M[Seq[Message]]

  /**
   * Starts a job if it is in the `NotStarted` state, otherwise an error string
   * is returned.
   */
  def start(job: JobId, startedAt: DateTime = new DateTime): M[Either[String, Job]]

  /**
   * Cancels a job. This doesn't necessarily mean the job has actually stopped,
   * it is just used to let the worker know a cancellation was requested. It is
   * up to the worker to abort the job. A cancelled job may still be completed
   * normally or aborted for other reasons than the cancellation. The reason
   * should give a useful string about why the cancellation was requested (eg.
   * "User action." or "Server restart.").
   */
  def cancel(job: JobId, reason: String, cancelledAt: DateTime = new DateTime): M[Either[String, Job]]

  /**
   * Aborts a job by putting it into the `Aborted` state. This is a terminal
   * state.
   */
  def abort(job: JobId, reason: String, abortedAt: DateTime = new DateTime): M[Either[String, Job]]

  /**
   * Moves the job to the `Finished` terminal state, with the given value as
   * the result.
   */
  def finish(job: JobId, finishedAt: DateTime = new DateTime): M[Either[String, Job]]

  /**
   * Moves the job to the `Expired` terminal state.
   */
  def expire(job: JobId, expiredAt: DateTime = new DateTime): M[Either[String, Job]]

  def setResult(job: JobId, mimeType: Option[MimeType], data: StreamT[M, Array[Byte]]): M[Either[String, Unit]]

  def getResult(job: JobId): M[Either[String, (Option[MimeType], StreamT[M, Array[Byte]])]]

  def withM[N[+_]](implicit t: M ~> N, u: N ~> M, M: Monad[M], N: Monad[N]) = new JobManager[N] {
    import scalaz.syntax.monad._

    private val transformStreamBack = implicitly[Hoist[StreamT]].hoist(u)
    private val transformStreamForward = implicitly[Hoist[StreamT]].hoist(t)

    def createJob(auth: APIKey, name: String, jobType: String, data: Option[JValue], started: Option[DateTime]): N[Job] =
      t(self.createJob(auth, name, jobType, data, started))

    def findJob(job: JobId): N[Option[Job]] = t(self.findJob(job))

    def listJobs(apiKey: APIKey): N[Seq[Job]] = t(self.listJobs(apiKey))

    def updateStatus(job: JobId, prevStatus: Option[StatusId], msg: String, progress: BigDecimal, unit: String, extra: Option[JValue]): N[Either[String, Status]] =
      t(self.updateStatus(job, prevStatus, msg, progress, unit, extra))

    def getStatus(job: JobId): N[Option[Status]] = t(self.getStatus(job))

    def listChannels(job: JobId): N[Seq[ChannelId]] = t(self.listChannels(job))

    def addMessage(job: JobId, channel: ChannelId, value: JValue): N[Message] = t(self.addMessage(job, channel, value))

    def listMessages(job: JobId, channel: ChannelId, since: Option[MessageId]): N[Seq[Message]] = t(self.listMessages(job, channel, since))

    def start(job: JobId, startedAt: DateTime = new DateTime): N[Either[String, Job]] = t(self.start(job, startedAt))

    def cancel(job: JobId, reason: String, cancelledAt: DateTime = new DateTime): N[Either[String, Job]] = t(self.cancel(job, reason, cancelledAt))

    def abort(job: JobId, reason: String, abortedAt: DateTime = new DateTime): N[Either[String, Job]] = t(self.abort(job, reason, abortedAt))

    def finish(job: JobId, finishedAt: DateTime = new DateTime): N[Either[String, Job]] = t(self.finish(job, finishedAt))

    def expire(job: JobId, expiredAt: DateTime = new DateTime): N[Either[String, Job]] = t(self.expire(job, expiredAt))

    def setResult(job: JobId, mimeType: Option[MimeType], data: StreamT[N, Array[Byte]]): N[Either[String, Unit]] =
      t(self.setResult(job, mimeType, transformStreamBack(data)))

    def getResult(job: JobId): N[Either[String, (Option[MimeType], StreamT[N, Array[Byte]])]] = t(self.getResult(job)) map {
      case Left(s) => Left(s)
      case Right((mimeType, data)) => Right((mimeType, transformStreamForward(data)))
    }
  }
}


/**
 * Given a method that can transition a Job between states, this provides
 * default implementations of the explicit state transition methods.
 */
trait JobStateManager[M[+_]] { self: JobManager[M] =>
  import JobState._

  protected def transition(job: JobId)(t: JobState => Either[String, JobState]): M[Either[String, Job]]

  def start(id: JobId, startTime: DateTime): M[Either[String, Job]] = transition(id) {
    case NotStarted => Right(Started(startTime, NotStarted))
    case badState => Left("Cannot start job. %s" format JobState.describe(badState))
  }

  def cancel(id: JobId, reason: String, cancelledAt: DateTime = new DateTime): M[Either[String, Job]] = transition(id) {
    case prev @ (NotStarted | Started(_, _)) => Right(Cancelled(reason, cancelledAt, prev))
    case badState => Left(JobState.describe(badState))
  }

  def abort(id: JobId, reason: String, abortedAt: DateTime = new DateTime): M[Either[String, Job]] = transition(id) {
    case prev @ (NotStarted | Started(_, _) | Cancelled(_, _, _)) =>
      Right(Aborted(reason, abortedAt, prev))
    case badState =>
      Left("Job already in terminal state. %s" format JobState.describe(badState))
  }

  def finish(id: JobId, finishedAt: DateTime = new DateTime): M[Either[String, Job]] = transition(id) {
    case prev @ (NotStarted | Started(_, _) | Cancelled(_, _, _)) =>
      Right(Finished(finishedAt, prev))
    case badState =>
      Left("Job already in terminal state. %s" format JobState.describe(badState))
  }

  def expire(id: JobId, expiredAt: DateTime = new DateTime): M[Either[String, Job]] = transition(id) {
    case prev @ (NotStarted | Started(_, _) | Cancelled(_, _, _)) =>
      Right(Expired(expiredAt, prev))
    case badState =>
      Left("Job already in terminal state. %s" format JobState.describe(badState))
  }
}

trait JobResultManager[M[+_]] { self: JobManager[M] =>
  import scalaz.syntax.monad._

  implicit def M: Monad[M]
  protected def fs: FileStorage[M]

  def setResult(id: JobId, mimeType: Option[MimeType], data: StreamT[M, Array[Byte]]): M[Either[String, Unit]] = {
    findJob(id) flatMap (_ map { job =>
      fs.save(job.id, FileData(mimeType, data)) map (Right(_))
    } getOrElse M.point(Left("Invalid job id: " + id)))
  }

  def getResult(job: JobId): M[Either[String, (Option[MimeType], StreamT[M, Array[Byte]])]] = {
    fs.load(job) map (_ map { case FileData(mimeType, data) =>
      Right((mimeType, data))
    } getOrElse {
      Left("No results exist for job " + job)
    })
  }
}
