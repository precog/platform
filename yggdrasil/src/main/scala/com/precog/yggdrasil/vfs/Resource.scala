package com.precog
package yggdrasil
package vfs

object Resource {
  val QuirrelData = MimeType("application", "x-quirrel-data")
}

sealed trait Resource {
  def mimeType: Future[MimeType]
  def authorities: Future[Authorities]
  def close: Future[PrecogUnit]
  def append(data: PathData): Future[PrecogUnit]
}

sealed trait ResourceError {
  def message: String
}

object ResourceError {
  case class CorruptData(message: String) extends ResourceError
  case class IOError(source: Exception) extends ResourceError

  def fromExtractorError(msg: String): Extractor.Error => ResourceError = { error =>
    CorruptData("%s:\n%s" format (msg, error.message))
  }
}
