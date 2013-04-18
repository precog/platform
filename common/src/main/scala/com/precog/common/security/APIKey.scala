package com.precog.common
package security

import blueeyes.json.JValue
import blueeyes.json.serialization.{ Decomposer, Extractor }
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Versioned._

import scalaz.Scalaz._
import scalaz.Validation

import shapeless._

case class APIKeyRecord(
  apiKey:         APIKey,
  name:           Option[String],
  description:    Option[String],
  issuerKey:      APIKey,
  grants:         Set[GrantId],
  isRoot:         Boolean)

object APIKeyRecord {
  implicit val apiKeyRecordIso = Iso.hlist(APIKeyRecord.apply _, APIKeyRecord.unapply _)

  val schemaV1 =       "apiKey"  :: "name" :: "description" :: ("issuerKey" ||| "(undefined)") :: "grants" :: "isRoot" :: HNil

  @deprecated("V0 serialization schemas should be removed when legacy data is no longer needed", "2.1.5")
  val schemaV0 =       "tid"     :: "name" :: "description" :: ("cid" ||| "(undefined)") :: "gids" :: ("isRoot" ||| false) :: HNil

  val decomposerV1: Decomposer[APIKeyRecord]= decomposerV[APIKeyRecord](schemaV1, Some("1.0".v))
  val extractorV2: Extractor[APIKeyRecord] = extractorV[APIKeyRecord](schemaV1, Some("1.0".v))
  val extractorV1: Extractor[APIKeyRecord] = extractorV[APIKeyRecord](schemaV1, None)
  val extractorV0: Extractor[APIKeyRecord]  = extractorV[APIKeyRecord](schemaV0, None)

  implicit val decomposer: Decomposer[APIKeyRecord] = decomposerV1
  implicit val extractor: Extractor[APIKeyRecord] = extractorV2 <+> extractorV1 <+> extractorV0
}
// vim: set ts=4 sw=4 et:
