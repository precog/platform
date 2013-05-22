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
