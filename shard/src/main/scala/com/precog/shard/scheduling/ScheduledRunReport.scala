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
package com.precog.shard
package scheduling

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Versioned._
import blueeyes.json.serialization.DefaultSerialization._

import java.util.UUID

import org.joda.time.DateTime

import scalaz._

import shapeless._

object ScheduledRunReport {
  import com.precog.common.ingest.JavaSerialization._

  implicit val iso = Iso.hlist(ScheduledRunReport.apply _, ScheduledRunReport.unapply _)

  val schemaV1 = "id" :: "startedAt" :: "endedAt" :: "records" :: "messages" :: HNil

  implicit val decomposer = decomposerV[ScheduledRunReport](schemaV1, Some("1.0".v))
  implicit val extractor  = extractorV[ScheduledRunReport](schemaV1, Some("1.0".v))
}

case class ScheduledRunReport(id: UUID, startedAt: DateTime, endedAt: DateTime, records: Long, messages: List[String] = Nil)
