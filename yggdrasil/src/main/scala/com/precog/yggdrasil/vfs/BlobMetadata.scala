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
package com.precog.yggdrasil
package vfs

import com.precog.common.security.Authorities

import org.joda.time.DateTime

import blueeyes.core.http.{MimeType, MimeTypes}
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._
//import blueeyes.json.serialization.IsoSerialization._

import shapeless.{ Iso, HNil }
import scalaz.syntax.std.option._

case class BlobMetadata(mimeType: MimeType, size: Long, created: DateTime, authorities: Authorities)

object BlobMetadata {
  implicit val iso = Iso.hlist(BlobMetadata.apply _, BlobMetadata.unapply _)

  implicit val mimeTypeDecomposer: Decomposer[MimeType] = new Decomposer[MimeType] {
    def decompose(u: MimeType) = JString(u.toString)
  }

  implicit val mimeTypeExtractor: Extractor[MimeType] = new Extractor[MimeType] {
    def validated(jv: JValue) = jv.validated[String].flatMap { ms =>
      MimeTypes.parseMimeTypes(ms).headOption.toSuccess(Error.invalid("Could not extract mime type from '%s'".format(ms)))
    }
  }

  val schema = "mimeType" :: "size" :: "created" :: "authorities" :: HNil
  implicit val decomposer = decomposerV[BlobMetadata](schema, Some("1.0".v))
  implicit val extractor = extractorV[BlobMetadata](schema, Some("1.0".v))
}



