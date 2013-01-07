package com.precog.common
package jobs

import blueeyes.core.http.{ MimeType, MimeTypes }

import scalaz.StreamT

case class FileData[M[+_]](mimeType: Option[MimeType], data: StreamT[M, Array[Byte]])

/**
 * An abstraction for storing/manipulating/retrieving files.
 */
trait FileStorage[M[+_]] {
  def exists(file: String): M[Boolean]
  def save(file: String, data: FileData[M]): M[Unit]
  def load(file: String): M[Option[FileData[M]]]
  def remove(file: String): M[Unit]
}
