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

import com.precog.common.security._

import blueeyes.json._
import blueeyes.json.serialization.{ Decomposer, Extractor }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Versioned._

import blueeyes.json.{ serialization => _, _ }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import org.joda.time.DateTime

import shapeless._

import scalaz._
import scalaz.syntax.std.boolean._

case class Job(id: JobId, apiKey: APIKey, name: String, jobType: String, data: Option[JValue], state: JobState)
object Job {
  implicit val iso = Iso.hlist(Job.apply _, Job.unapply _)
  val schemaV1 = "id" :: "apiKey" :: "name" :: "type" :: "data" :: "state" :: HNil
  implicit val decomposerV1: Decomposer[Job] = decomposerV[Job](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[Job] = extractorV[Job](schemaV1, Some("1.0".v))
}

case class Message(job: JobId, id: MessageId, channel: String, value: JValue)
object Message {
  object channels {
    val Status = "status"
    val Errors = "errors"
    val Warnings = "warnings"
  }

  implicit val iso = Iso.hlist(Message.apply _, Message.unapply _)
  val schemaV1 = "jobId" :: "id" :: "channel" :: "value" :: HNil
  implicit val decomposerV1: Decomposer[Message] = decomposerV[Message](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[Message] = extractorV[Message](schemaV1, Some("1.0".v))
}

case class Status(job: JobId, id: StatusId, message: String, progress: BigDecimal, unit: String, info: Option[JValue])
object Status {
  import JobManager._
  import scalaz.syntax.apply._

  implicit val iso = Iso.hlist(Status.apply _, Status.unapply _)
  val schemaV1 = "job" :: "id" :: "message" :: "progress" :: "unit" :: "info" :: HNil
  implicit val decomposerV1: Decomposer[Status] = decomposerV[Status](schemaV1, Some("1.0".v))
  implicit val extractorV1: Extractor[Status] = extractorV[Status](schemaV1, Some("1.0".v))

  def fromMessage(message: Message): Option[Status] = {
    (message.channel == channels.Status) option {
      ((message.value \ "message").validated[String] |@|
       (message.value \ "progress").validated[BigDecimal] |@|
       (message.value \ "unit").validated[String]) { (msg, progress, unit) =>
        Status(message.job, message.id, msg, progress, unit, message.value \? "info")
      }
    } flatMap {
      _.toOption
    } 
  }

  def toMessage(status: Status): Message = {
    Message(status.job, status.id, channels.Status, JObject(
      jfield("message", status.message) ::
      jfield("progress", status.progress) ::
      jfield("unit", status.unit) ::
      (status.info map (jfield("info", _) :: Nil) getOrElse Nil)
    ))
  }
}
