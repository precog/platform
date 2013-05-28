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
package ingest

import com.precog.common.serialization._

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.core.http.{ MimeTypes, MimeType }
import IsoSerialization._
import DefaultSerialization._
import Versioned._
import Extractor._

import scalaz._
import scalaz.syntax.apply._
import scalaz.syntax.std.option._

sealed trait ContentEncoding {
  def id: String
  def encode(raw: Array[Byte]): String
  def decode(compressed: String): Array[Byte]
}

object ContentEncoding {
  val decomposerV1: Decomposer[ContentEncoding] = new Decomposer[ContentEncoding] {
    def decompose(ce: ContentEncoding) = JObject("encoding" -> ce.id.serialize)
  }

  val extractorV1: Extractor[ContentEncoding] = new Extractor[ContentEncoding] {
    override def validated(obj: JValue): Validation[Error, ContentEncoding] = {
      obj.validated[String]("encoding").flatMap {
        case "uncompressed" => Success(RawUTF8Encoding)
        case invalid => Failure(Invalid("Unknown encoding " + invalid))
      }
    }
  }

  implicit val decomposer = decomposerV1.versioned(Some("1.0".v))
  implicit val extractor = extractorV1.versioned(Some("1.0".v))
}

object RawUTF8Encoding extends ContentEncoding {
  val id = "uncompressed"
  def encode(raw: Array[Byte]) = new String(raw, "UTF-8")
  def decode(compressed: String) = compressed.getBytes("UTF-8")
}

case class FileContent(data: Array[Byte], mimeType: MimeType, encoding: ContentEncoding)

object FileContent {
  import MimeTypes._
  val XQuirrelData = MimeType("application", "x-quirrel-data")
  val XQuirrelScript = MimeType("text", "x-quirrel-script")
  val XJsonStream = MimeType("application", "x-json-stream")
  val ApplicationJson = application/json
  val TextCSV = text/csv
  val TextPlain = text/plain
  val AnyMimeType = MimeType("*", "*")

  val stringTypes = Set(XQuirrelScript, ApplicationJson, TextCSV, TextPlain)

  val DecomposerV0: Decomposer[FileContent] = new Decomposer[FileContent] {
    def decompose(v: FileContent) = JObject(
      "data" -> JString(v.encoding.encode(v.data)),
      "mimeType" -> v.mimeType.jv,
      "encoding" -> v.encoding.jv
    )
  }

  val ExtractorV0: Extractor[FileContent] = new Extractor[FileContent] {
    def validated(jv: JValue) = {
      jv match {
        case JObject(fields) =>
          (fields.get("encoding").toSuccess(Invalid("File data object missing encoding field.")).flatMap(_.validated[ContentEncoding]) |@|
           fields.get("mimeType").toSuccess(Invalid("File data object missing MIME type.")).flatMap(_.validated[MimeType]) |@|
           fields.get("data").toSuccess(Invalid("File data object missing data field.")).flatMap(_.validated[String])) { (encoding, mimeType, contentString) =>
            FileContent(encoding.decode(contentString), mimeType, encoding)
          }

        case _ =>
          Failure(Invalid("File contents " + jv.renderCompact + " was not properly encoded as a JSON object."))
      }
    }
  }

  implicit val decomposer = DecomposerV0
  implicit val extractor = ExtractorV0
}
