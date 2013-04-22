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
