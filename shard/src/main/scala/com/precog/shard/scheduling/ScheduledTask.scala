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
import com.precog.daze.QueryOptions

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

case class ScheduledTask(id: UUID, repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, prefix: Path, source: Path, sink: Path, timeoutMillis: Option[Long]) {
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
