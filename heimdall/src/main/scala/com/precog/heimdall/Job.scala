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
    Message(status.job, status.id, Message.channels.Status, JObject(List(
      JField("message", status.message),
      JField("progress", status.progress),
      JField("unit", status.unit),
      JField("info", status.info getOrElse JUndefined)
    )))
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

