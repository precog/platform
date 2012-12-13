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

import com.precog.common.json._
import com.precog.common.security._

import blueeyes.json.{ serialization => _, _ }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import org.joda.time.DateTime

import shapeless._

import scalaz._

case class Job(id: JobId, apiKey: APIKey, name: String, jobType: String, data: Option[JValue], state: JobState)

case class Status(job: JobId, id: StatusId, message: String, progress: BigDecimal, unit: String, info: Option[JValue])
case class Message(job: JobId, id: MessageId, channel: String, value: JValue)

object Job {
  implicit val jobIso = Iso.hlist(Job.apply _, Job.unapply _)

  val schema = "id" :: "apiKey" :: "name" :: "type" :: "data" :: "state" :: HNil

  implicit val (decomposer, extractor) = serialization[Job](schema)
}

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

object Message {
  object channels {
    val Status = "status"
    val Errors = "errors"
    val Warnings = "warnings"
  }

  implicit val messageIso = Iso.hlist(Message.apply _, Message.unapply _)

  val schema = "jobId" :: "id" :: "channel" :: "value" :: HNil

  implicit val (decomposer, extractor) = serialization[Message](schema)
}
