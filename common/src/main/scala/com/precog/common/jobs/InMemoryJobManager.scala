package com.precog.common
package jobs

import blueeyes.json._
import blueeyes.core.http.MimeType

import com.precog.common.security._

import java.util.UUID

import org.joda.time.DateTime

import scala.collection.mutable

import scalaz._

// private case class JobState(job: Job, channels: Map[String, List[Message]], status: List[Status])

final class InMemoryJobManager[M[+_]](implicit val M: Monad[M])
    extends JobManager[M] with JobStateManager[M] with JobResultManager[M] {
  import scalaz.syntax.monad._
  import JobState._

  val fs = new InMemoryFileStorage[M]

  val jobs: mutable.Map[JobId, Job] = new mutable.HashMap[JobId, Job] with mutable.SynchronizedMap[JobId, Job]

  val channels: mutable.Map[(JobId, String), List[Message]] = new mutable.HashMap[(JobId, String), List[Message]] with mutable.SynchronizedMap[(JobId, String), List[Message]]

  val statuses: mutable.Map[JobId, List[Status]] = new mutable.HashMap[JobId, List[Status]] with mutable.SynchronizedMap[JobId, List[Status]]

  val status: mutable.Map[JobId, Status] = new mutable.HashMap[JobId, Status] with mutable.SynchronizedMap[JobId, Status]

  //val result: mutable.Map[JobId, (List[MimeType], StreamT[M, Array[Byte]])] =
  //  new mutable.HashMap[JobId, (List[MimeType], StreamT[M, Array[Byte]])] with mutable.SynchronizedMap[JobId, (List[MimeType], StreamT[M, Array[Byte]])]

  private def newJobId: JobId = UUID.randomUUID().toString.toLowerCase.replace("-", "")

  def createJob(auth: APIKey, name: String, jobType: String, data: Option[JValue], started: Option[DateTime]): M[Job] = {
    M.point {
      val state = started map (Started(_, NotStarted)) getOrElse NotStarted
      val job = Job(newJobId, auth, name, jobType, data, state)
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
        for (m <- addMessage(jobId, JobManager.channels.Status, jval)) yield {
          val Some(s) = Status.fromMessage(m)
          status.put(jobId, s)
          Right(s)
        }

      case Some(_) =>
        M.point(Left("Current status did not match expected status."))

      case None if prevStatus.isDefined =>
        M.point(Left("Job has not yet started, yet a status was expected."))

      case None =>
        for (m <- addMessage(jobId, JobManager.channels.Status, jval)) yield {
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
