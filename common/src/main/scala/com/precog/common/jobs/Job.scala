package com.precog.common
package jobs

import blueeyes.json._
import blueeyes.json.serialization.{ Decomposer, Extractor, ValidatedExtraction }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import com.precog.common.security._

import org.joda.time.DateTime

import scalaz._

case class Job(id: JobId, apiKey: APIKey, name: String, jobType: String, state: JobState, expires: Option[DateTime])

case class Status(job: JobId, id: StatusId, message: String, progress: BigDecimal, unit: String, info: Option[JValue])

case class Message(job: JobId, id: MessageId, channel: String, value: JValue)

object Job extends JobSerialization

object Status {
  import scalaz.syntax.apply._

  def fromMessage(message: Message): Option[Status] = {
    if (message.channel == Message.channels.Status) {
      ((message.value \ "message").validated[String] |@|
       (message.value \ "progress").validated[BigDecimal] |@|
       (message.value \ "unit").validated[String])({ (msg, progress, unit) =>

        Status(message.job, message.id, msg, progress, unit, message.value \? "info")
      }).toOption
    } else None
  }

  def toMessage(status: Status): Message = {
    Message(status.job, status.id, Message.channels.Status, JObject(
      JField("message", status.message) ::
      JField("progress", status.progress) ::
      JField("unit", status.unit) ::
      (status.info map (JField("info", _) :: Nil) getOrElse Nil)
    ))
  }
}

object Message extends MessageSerialization {
  object channels {
    val Status = "status"
    val Errors = "errors"
    val Warnings = "warnings"
  }
}

trait JobSerialization {
  import Extractor._
  import scalaz.Validation._
  import scalaz.syntax.apply._
  import scalaz.std.option._

  implicit object JobDecomposer extends Decomposer[Job] {
    override def decompose(job: Job): JValue = JObject(List(
      JField("id", JString(job.id)),
      JField("apiKey", JString(job.apiKey)),
      JField("name", JString(job.name)),
      JField("type", JString(job.jobType)),
      JField("state", job.state.serialize),
      JField("expires", job.expires map (_.serialize) getOrElse JNull)
    ))
  }

  implicit object JobExtractor extends Extractor[Job] with ValidatedExtraction[Job] {
    import scalaz.syntax.plus._

    override def validated(obj: JValue): Validation[Error, Job] = {
      ((obj \ "id").validated[JobId] |@|
       (obj \ "apiKey").validated[APIKey] |@|
       (obj \ "name").validated[String] |@|
       (obj \ "type").validated[String] |@|
       (obj \ "state").validated[JobState] |@|
       ((obj \ "expires").validated[DateTime].map(some(_)) <+> success(none)))(Job.apply _)
    }
  }
}

trait MessageSerialization {
  import Extractor._
  import scalaz.syntax.apply._

  implicit object MessageDecomposer extends Decomposer[Message] {
    override def decompose(msg: Message): JValue = JObject(List(
      JField("id", JNum(msg.id)),
      JField("jobId", JString(msg.job)),
      JField("channel", JString(msg.channel)),
      JField("value", msg.value)
    ))
  }

  implicit object MessageExtractor extends Extractor[Message] with ValidatedExtraction[Message] {
    override def validated(obj: JValue): Validation[Error, Message] = {
      ((obj \ "jobId").validated[JobId] |@|
       (obj \ "id").validated[MessageId] |@|
       (obj \ "channel").validated[String])(Message(_, _, _, obj \ "value"))
    }
  }
}

