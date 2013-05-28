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

import akka.util.Duration

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Versioned._
import blueeyes.json.serialization.DefaultSerialization._

import com.precog.common.Path
import com.precog.common.ingest.JavaSerialization._
import com.precog.common.security.{APIKey, Authorities}
import com.precog.daze.{ QueryOptions, EvaluationContext }

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.quartz.CronExpression

import scalaz._

import shapeless._

object CronExpressionSerialization {
  implicit val cronExpressionDecomposer = new Decomposer[CronExpression] {
    def decompose(expr: CronExpression) = JString(expr.toString)
  }

  implicit val cronExpressionExtractor = new Extractor[CronExpression] {
    def validated(jv: JValue) = jv match {
      case JString(expr) => Validation.fromTryCatch(new CronExpression(expr)).leftMap(Extractor.Error.thrown)
      case invalid => Failure(Extractor.Error.invalid("Could not parse CRON expression from " + invalid))
    }
  }
}

case class ScheduledTask(id: UUID, repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]) {
  def taskName = "Scheduled %s -> %s".format(source, sink)
  def timeout = timeoutMillis map { to => Duration(to, TimeUnit.MILLISECONDS) }
}

object ScheduledTask {
  import CronExpressionSerialization._

  implicit val iso = Iso.hlist(ScheduledTask.apply _, ScheduledTask.unapply _)

  val schemaV1 = "id" :: "repeat" :: "apiKey" :: "authorities" :: "prefix" :: "source" :: "sink" :: "timeout" :: HNil

  implicit val decomposer: Decomposer[ScheduledTask] = decomposerV(schemaV1, Some("1.0".v))
  implicit val extractor:  Extractor[ScheduledTask]  = extractorV(schemaV1, Some("1.0".v))
}
