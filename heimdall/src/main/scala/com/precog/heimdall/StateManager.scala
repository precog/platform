package com.precog.heimdall

import blueeyes.json._
import JsonAST._

import com.precog.common.security._

import org.joda.time.DateTime

import scalaz._


trait JobManager[M[+_]] {
  import Message._

  implicit def M: Monad[M]

  /**
   * Create a new Job with the given API key, name, type and possibly an
   * initial status message and expiration. If a started time is provided, then
   * the job will be put in the Started state, otherwise it will be in the
   * NotStarted state until `start(...)` is run.
   */
  def createJob(auth: APIKey, name: String, jobType: String, started: Option[DateTime], expires: Option[DateTime]): M[Job]

  /** 
   * Returns the Job with the given ID if it exists.
   */
  def findJob(job: JobId): M[Option[Job]]

  /**
   * Returns a list of all currently running jobs
   */
  def listJobs(show: JobState => Boolean = (_ => true)): M[Seq[Job]]

  /**
   * Updates a job's status to `value`. If a `prevStatus` is provided, then
   * this must match the current status in order for the update to succeed,
   * otherwise the update fails and the actual current status is returned.
   */
  def updateStatus(job: JobId, prevStatus: Option[StatusId], msg: String, progress: Double, unit: String, extra: Option[JValue]): M[Either[String, Status]]

  /**
   * Returns just the latest status message.
   */
  def getStatus(job: JobId): M[Option[Status]]

  /**
   * Lists all channels that have had messages posted to them. Note that
   * channels are created on a demand by `addMessage`, so 
   */
  def listChannels(job: JobId): M[Seq[String]]

  /**
   * Add a message to a job's channel. If the channel does not exist, it will
   * be created.
   */
  def addMessage(job: JobId, channel: String, value: JValue): M[Message]

  /**
   * Returns all the messages posted to a job's channel since some specified
   * message. If no previous message is given, then all messages posted to the
   * given channel are returned.
   */
  def listMessages(job: JobId, channel: String, since: Option[MessageId]): M[Seq[Message]]

  /**
   * Starts a job if it is in the `NotStarted` state, otherwise an error string
   * is returned.
   */
  def start(job: JobId, startTime: DateTime): M[Either[String, Job]]

  /**
   * Cancels a job. This doesn't necessarily mean the job has actually stopped,
   * it is just used to let the worker know a cancellation was requested. It is
   * up to the worker to abort the job. A cancelled job may still be completed
   * normally or aborted for other reasons than the cancellation. The reason
   * should give a useful string about why the cancellation was requested (eg.
   * "User action." or "Server restart.").
   */
  def cancel(job: JobId, reason: String): M[Either[String, Job]]

  /**
   * Aborts a job by putting it into the `Aborted` state. This is a terminal
   * state.
   */
  def abort(job: JobId, reason: String): M[Either[String, Job]]

  /**
   * Moves the job to the `Finished` terminal state, with the given value as
   * the result.
   */
  def finish[A](job: JobId, result: A): M[Either[String, Job]]
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

  def cancel(id: JobId, reason: String): M[Either[String, Job]] = transition(id) {
    case prev @ (NotStarted | Started(_, _)) => Right(Cancelled(reason, prev))
    case badState => Left(JobState.describe(badState))
  }

  def abort(id: JobId, reason: String): M[Either[String, Job]] = transition(id) {
    case prev @ (NotStarted | Started(_, _) | Cancelled(_, _)) =>
      Right(Aborted(reason, prev))
    case badState =>
      Left("Job already in terminal state. %s" format JobState.describe(badState))
  }

  def finish[A](id: JobId, result: A): M[Either[String, Job]] = transition(id) {
    case prev @ (NotStarted | Started(_, _) | Cancelled(_, _)) =>
      Right(Finished(result, prev))
    case badState =>
      Left("Job already in terminal state. %s" format JobState.describe(badState))
  }
}

