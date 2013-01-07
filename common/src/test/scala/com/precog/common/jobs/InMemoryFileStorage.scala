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
package jobs

import scala.collection.mutable

import blueeyes.core.http.{ MimeType, MimeTypes }

import scalaz._

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



