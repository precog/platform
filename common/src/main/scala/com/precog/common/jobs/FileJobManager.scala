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

import akka.util.Duration

import blueeyes.core.http.MimeTypes
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._

import com.google.common.cache.RemovalCause

import com.precog.common.security._
import com.precog.util._
import com.precog.util.cache.{Cache, SimpleCache}

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import scalaz._
import scalaz.syntax.id._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

import shapeless._

import java.io._
import java.util.UUID

import scala.collection.mutable

object FileJobManager {
  def apply[M[+_]](workDir: File, monadM: Monad[M]): FileJobManager[M] = {
    assert(workDir.isDirectory)
    assert(workDir.canWrite)

    new FileJobManager(workDir, monadM)
  }
}

class FileJobManager[M[+_]] private[FileJobManager] (workDir: File, monadM: Monad[M])
    extends JobManager[M]
    with JobStateManager[M]
    with JobResultManager[M]
    with FileStorage[M]
    with Logging {

  import JobState._

  implicit def M = monadM

  val fs = this

  import Cache._
  private[this] val cache: SimpleCache[JobId, FileJobState] = Cache.simple[JobId, FileJobState](
    ExpireAfterAccess(Duration(5, "minutes")),
    OnRemoval({ (jobId, job, reason) =>
      if (reason != RemovalCause.REPLACED) {
        // Make sure to save expired entries
        saveJob(jobId, job)
      }
      PrecogUnit
    })
  )

  def shutdown = cache.invalidateAll

  private[this] def newJobId: JobId = UUID.randomUUID().toString.toLowerCase.replace("-", "")

  private final val JOB_SUFFIX = ".job"
  private final val RESULT_SUFFIX = ".result"

  private[this] def jobFile(jobId: JobId) = new File(workDir, jobId + JOB_SUFFIX)
  private[this] def resultFile(jobId: JobId) = new File(workDir, jobId + RESULT_SUFFIX)

  private[this] def saveJob(jobId: JobId, jobState: FileJobState) = {
    cache += (jobId -> jobState)
    IOUtils.safeWriteToFile(jobState.serialize.renderCompact,  jobFile(jobId)).unsafePerformIO
  }

  private[this] def loadJob(jobId: JobId): Option[FileJobState] = {
    cache.get(jobId) orElse {
      if (jobFile(jobId).exists) {
        JParser.parseFromFile(jobFile(jobId)).bimap(Extractor.Thrown(_), j => j).flatMap { jobV => jobV.validated[FileJobState] }.bimap({
          error => logger.error("Error loading job for %s: %s".format(jobId, error.message))
        }, j => j).toOption
      } else {
        None
      }
    }
  }

  def createJob(auth: APIKey, name: String, jobType: String, data: Option[JValue], started: Option[DateTime]) = M.point {
    Job(newJobId, auth, name, jobType, data, started map (Started(_, NotStarted)) getOrElse NotStarted) unsafeTap { job =>
      val jobState = FileJobState(job, None, Map.empty)
      saveJob(job.id, jobState)
    }
  }

  def findJob(jobId: JobId): M[Option[Job]] = M.point {
    loadJob(jobId).map(_.job)
  }

  private val jobFileFilter = new FilenameFilter {
    def accept(dir: File, name: String) = name.endsWith(JOB_SUFFIX)
  }

  def listJobs(apiKey: APIKey): M[Seq[Job]] = M.point {
    // Load all jobs from the workDir that aren't in the cache
    Option(workDir.list(jobFileFilter)).foreach { names =>
      names.foreach { name => loadJob(name.substring(0, name.length - JOB_SUFFIX.length)) }
    }
    cache.values.collect {
      case FileJobState(job, _, _) if job.apiKey == apiKey => job
    }.toSeq
  }

  def listChannels(jobId: JobId): M[Seq[ChannelId]] = M.point {
    loadJob(jobId).map(_.messages.keys.toSeq).getOrElse(Seq.empty)
  }

  def addMessage(jobId: JobId, channel: ChannelId, value: JValue): M[Message] = M.point {
    loadJob(jobId) match {
      case Some(js @ FileJobState(_, _, messages)) =>
        val prior: List[Message] = messages.get(channel).getOrElse(Nil)
        val message = Message(jobId, prior.size, channel, value)

        cache += (jobId -> js.copy(messages = (messages + (channel -> (message :: prior)))))
        message

      case None =>
        throw new Exception("Message add attempt on non-existant job Id " + jobId)
    }
  }

  def listMessages(jobId: JobId, channel: ChannelId, since: Option[MessageId]): M[Seq[Message]] = M.point {
    val posts = cache.get(jobId).flatMap(_.messages.get(channel)).getOrElse(Nil)
    since map { mId => posts.takeWhile(_.id != mId).reverse } getOrElse posts.reverse
  }

  def updateStatus(jobId: JobId, prevStatus: Option[StatusId],
      msg: String, progress: BigDecimal, unit: String, extra: Option[JValue]): M[Either[String, Status]] = {

    val jval = JObject(
      JField("message", JString(msg)) ::
      JField("progress", JNum(progress)) ::
      JField("unit", JString(unit)) ::
      (extra map (JField("info", _) :: Nil) getOrElse Nil)
    )

    cache.get(jobId).map {
      case FileJobState(_, status, _) => status match {
        case Some(curStatus) if curStatus.id == prevStatus.getOrElse(curStatus.id) =>
          for (m <- addMessage(jobId, JobManager.channels.Status, jval)) yield {
            val Some(s) = Status.fromMessage(m)
            cache += (jobId -> cache(jobId).copy(status = Some(s)))
            Right(s)
          }

        case Some(_) =>
          M.point(Left("Current status did not match expected status."))

        case None if prevStatus.isDefined =>
          M.point(Left("Job has not yet started, yet a status was expected."))

        case None =>
          for (m <- addMessage(jobId, JobManager.channels.Status, jval)) yield {
            val Some(s) = Status.fromMessage(m)
            cache += (jobId -> cache(jobId).copy(status = Some(s)))
            Right(s)
          }
      }
    }.getOrElse(M.point(Left("No job found for jobId " + jobId)))
  }

  def getStatus(jobId: JobId): M[Option[Status]] = M.point {
    loadJob(jobId).flatMap(_.status)
  }

  def transition(jobId: JobId)(t: JobState => Either[String, JobState]): M[Either[String, Job]] = M.point {
    loadJob(jobId).toSuccess("Failed to locate job " + jobId).flatMap { fjs =>
      Validation.fromEither(t(fjs.job.state)).map { newState =>
        val updated = fjs.copy(job = fjs.job.copy(state = newState))
        saveJob(jobId, updated)
        updated.job
      }
    }.toEither
  }

  // Results handling
  def exists(file: String): M[Boolean] = M.point { resultFile(file).exists }

  def save(file: String, data: FileData[M]): M[Unit] = {
    // TODO: Make this more efficient
    data.data.toStream.map { chunks =>
      val output = new DataOutputStream(new FileOutputStream(resultFile(file)))

      try {
        output.writeUTF(data.mimeType.map(_.toString).getOrElse(""))
        val length = chunks.foldLeft(0)(_ + _.length)
        output.writeInt(length)
        chunks.foreach { bytes => output.write(bytes) }
      } finally {
        output.close()
      }
    }
  }

  def load(file: String): M[Option[FileData[M]]] = M.point {
    if (resultFile(file).exists) {
      val input = new DataInputStream(new FileInputStream(resultFile(file)))

      try {
        val mime = input.readUTF match {
          case "" => None
          case mimestring => MimeTypes.parseMimeTypes(mimestring).headOption
        }

        val length = input.readInt
        val data = new Array[Byte](length)
        if (input.read(data) != length) {
          throw new IOException("Incomplete data in " + resultFile(file))
        }

        Some(FileData(mime, data :: StreamT.empty[M, Array[Byte]]))
      } finally {
        input.close()
      }
    } else {
      None
    }
  }

  def remove(file: String): M[Unit] = M.point {
    val target = resultFile(file)
    if (target.exists && ! target.delete) {
      throw new IOException("Failed to delete job file: " + target.getCanonicalPath)
    }
  }
}

case class FileJobState(job: Job, status: Option[Status], messages: Map[ChannelId, List[Message]])

object FileJobState {
  val test = implicitly[Decomposer[Option[Status]]]

  implicit val fileJobStateIso = Iso.hlist(FileJobState.apply _, FileJobState.unapply _)

  val schemaV1 = "job" :: "status" :: "messages" :: HNil

  implicit val fjsDecomposerV1 : Decomposer[FileJobState] = decomposerV(schemaV1, Some("1.0".v))
  implicit val fjsExtractorV1 : Extractor[FileJobState] = extractorV(schemaV1, Some("1.0".v))
}
