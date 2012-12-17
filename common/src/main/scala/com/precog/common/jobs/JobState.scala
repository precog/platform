package com.precog.common
package jobs

import blueeyes.json._
import blueeyes.json.serialization.{ Decomposer, Extractor, ValidatedExtraction }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import com.precog.common.security._

import org.joda.time.DateTime

import scalaz._

/**
 * The Job state is used to keep track of the overall state of a Job. A Job is
 * put in a special initial state (`NotStarted`) when it is first created, and
 * is moved to the `Started` state once it gets its first status update. From
 * here it can either be `Cancelled` or put into one of several terminal
 * states. Once a job is in a terminal state, it can no longer be moved to a
 * new state.
 */
sealed abstract class JobState(val isTerminal: Boolean)

object JobState extends JobStateSerialization {
  case object NotStarted extends JobState(false)
  case class Started(timestamp: DateTime, prev: JobState) extends JobState(false)
  case class Cancelled(reason: String, timestamp: DateTime, prev: JobState) extends JobState(false)
  case class Aborted(reason: String, timestamp: DateTime, prev: JobState) extends JobState(true)
  case class Expired(timestamp: DateTime, prev: JobState) extends JobState(true)
  case class Finished(result: Option[JobResult], timestamp: DateTime, prev: JobState) extends JobState(true)

  def describe(state: JobState): String = state match {
    case NotStarted => "The job has not yet been started."
    case Started(started, _) => "The job was started at %s." format started
    case Cancelled(reason, _, _) => "The job has been cancelled due to '%s'." format reason
    case Aborted(reason, _, _) => "The job was aborted early due to '%s'." format reason
    case Expired(expiration, _) => "The job expired at %s." format expiration
    case Finished(_, _, _) => "The job has finished successfully."
  }
}

trait JobStateSerialization {
  import Extractor._
  import JobState._
  import scalaz.Validation._
  import scalaz.syntax.apply._

  implicit object JobStateDecomposer extends Decomposer[JobState] {
    private def base(state: String, timestamp: DateTime, previous: JobState, reason: Option[String] = None): JObject = {
      JObject(
        jfield("state", state) ::
        jfield("timestamp", timestamp) ::
        jfield("previous", decompose(previous)) ::
        (reason map { jfield("reason", _) :: Nil } getOrElse Nil)
      )
    }

    override def decompose(job: JobState): JValue = job match {
      case NotStarted =>
        jobject(jfield("state", "not_started"))

      case Started(ts, prev) =>
        base("started", ts, prev)

      case Cancelled(reason, ts, prev) =>
        base("cancelled", ts, prev, Some(reason))

      case Aborted(reason, ts, prev) =>
        base("aborted", ts, prev, Some(reason))

      case Expired(ts, prev) =>
        base("expired", ts, prev)

      case Finished(None, ts, prev) =>
        base("finished", ts, prev)

      case Finished(Some(result), ts, prev) =>
        base("finished", ts, prev).unsafeInsert(JPath("result"), result.serialize)
    }
  }

  implicit object JobStateExtractor extends Extractor[JobState] with ValidatedExtraction[JobState] {
    def extractBase(obj: JValue): Validation[Error, (DateTime, JobState)] = {
      ((obj \ "timestamp").validated[DateTime] |@| (obj \ "previous").validated[JobState]).tupled
    }

    override def validated(obj: JValue) = {
      (obj \ "state").validated[String] flatMap {
        case "not_started" =>
          success[Error, JobState](NotStarted)

        case "started" =>
          extractBase(obj) map (Started(_, _)).tupled

        case "cancelled" =>
          ((obj \ "reason").validated[String] |@| extractBase(obj)) { case (reason, (timestamp, previous)) =>
            Cancelled(reason, timestamp, previous)
          }

        case "aborted" =>
          ((obj \ "reason").validated[String] |@| extractBase(obj)) { case (reason, (timestamp, previous)) =>
            Aborted(reason, timestamp, previous)
          }

        case "expired" =>
          extractBase(obj) map (Expired(_, _)).tupled

        case "finished" =>
          extractBase(obj) flatMap { case (timestamp, previous) =>
            (obj \? "result") match {
              case Some(result) =>
                result.validated[JobResult] map { result => Finished(Some(result), timestamp, previous) }
              case None =>
                success(Finished(None, timestamp, previous))
            }
          }
      }
    }
  }
}

