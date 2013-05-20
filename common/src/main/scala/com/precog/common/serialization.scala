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

import blueeyes.core.http.{MimeType, MimeTypes}
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._

import java.util.UUID

import scalaz.{Failure, Success}

package object serialization {
  implicit val uuidDecomposer: Decomposer[UUID] = new Decomposer[UUID] {
    def decompose(u: UUID) = JString(u.toString)
  }

  implicit val uuidExtractor: Extractor[UUID] = new Extractor[UUID] {
    def validated(jv: JValue) = jv.validated[String].map(UUID.fromString)
  }

  implicit val mimeTypeDecomposer: Decomposer[MimeType] = new Decomposer[MimeType] {
    def decompose(m: MimeType) = JString(m.toString)
  }

  implicit val mimeTypeExtractor: Extractor[MimeType] = new Extractor[MimeType] {
    def validated(jv: JValue) = jv.validated[String] map(MimeTypes.parseMimeTypes(_).toList) flatMap {
      case Nil =>
        Failure(Extractor.Error.invalid("No mime types found in " + jv.renderCompact))

      case primary :: rest =>
        Success(primary)
    }
  }
}
