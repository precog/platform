package com.precog.daze

import org.joda.time.DateTime

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Versioned._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import com.precog.common._
import com.precog.common.security._
import com.precog.common.accounts._

import shapeless._

case class EvaluationContext(apiKey: APIKey, account: AccountDetails, basePath: Path, startTime: DateTime)

object EvaluationContext {
  implicit val iso = Iso.hlist(EvaluationContext.apply _, EvaluationContext.unapply _)

  val schemaV1 = "apiKey" :: "account" :: "basePath" :: "startTime" :: HNil

  implicit val decomposer: Decomposer[EvaluationContext] = decomposerV(schemaV1, Some("1.0".v))
  implicit val extractor:  Extractor[EvaluationContext]  = extractorV(schemaV1, Some("1.0".v))
}
