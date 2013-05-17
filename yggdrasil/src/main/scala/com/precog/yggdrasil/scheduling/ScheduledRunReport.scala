package com.precog.muspelheim
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
