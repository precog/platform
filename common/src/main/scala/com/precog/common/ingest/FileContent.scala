package com.precog.common
package ingest

import blueeyes.json._
import blueeyes.json.serialization._
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

case class FileContent(data: Array[Byte], encoding: ContentEncoding) 

object FileContent {
  val DecomposerV0: Decomposer[FileContent] = new Decomposer[FileContent] {
    def decompose(v: FileContent) = JObject(
      "encoding" -> v.encoding.jv,
      "data" -> JString(v.encoding.encode(v.data))
    )
  }

  val ExtractorV0: Extractor[FileContent] = new Extractor[FileContent] {
    def validated(jv: JValue) = {
      jv match {
        case JObject(fields) =>
          (fields.get("encoding").toSuccess(Invalid("File data object missing encoding field.")).flatMap(_.validated[ContentEncoding]) |@|
           fields.get("data").toSuccess(Invalid("File data object missing data field.")).flatMap(_.validated[String])) { (encoding, contentString) => 
            FileContent(encoding.decode(contentString), encoding)
          }

        case _ => 
          Failure(Invalid("File contents " + jv.renderCompact + " was not properly encoded as a JSON object."))
      }
    }
  }

  implicit val decomposer = DecomposerV0
  implicit val extractor = ExtractorV0
}



