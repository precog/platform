package com.precog.common.jobs

import com.precog.common._

import blueeyes.core.http.{ MimeType, MimeTypes }

import scala.collection.mutable

import scalaz._

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

final class InMemoryFileStorage[M[+_]](implicit M: Monad[M]) extends FileStorage[M] {
  import scalaz.syntax.monad._

  private val files = new mutable.HashMap[String, (Option[MimeType], Array[Byte])]
      with mutable.SynchronizedMap[String, (Option[MimeType], Array[Byte])]

  def exists(file: String): M[Boolean] = M.point { files contains file }

  def save(file: String, data: FileData[M]): M[Unit] = data.data.toStream map { chunks =>
    val length = chunks.foldLeft(0)(_ + _.length)
    val bytes = new Array[Byte](length)
    chunks.foldLeft(0) { (offset, chunk) =>
      System.arraycopy(chunk, 0, bytes, offset, chunk.length)
      offset + chunk.length
    }

    files += file -> (data.mimeType, bytes)
  }

  def load(file: String): M[Option[FileData[M]]] = M.point {
    files get file map { case (mimeType, data) =>
      FileData(mimeType, data :: StreamT.empty[M, Array[Byte]])
    }
  }

  def remove(file: String): M[Unit] = M.point { files -= file }
}
