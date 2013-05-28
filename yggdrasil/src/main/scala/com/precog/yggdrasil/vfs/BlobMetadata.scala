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



