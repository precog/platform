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
package com.precog.util

import java.io._

import scalaz.Validation
import scalaz.effect._

trait FileOps {
  def exists(src: File): IO[Boolean]

  def rename(src: File, dest: File): IO[Boolean]
  def copy(src: File, dest: File): IO[PrecogUnit]

  def read(src: File): IO[String]
  def write(dest: File, content: String): IO[PrecogUnit]

  def mkdir(dir: File): IO[Boolean]
}

object FilesystemFileOps extends FileOps {
  def exists(src: File) = IO { src.exists() }

  def rename(src: File, dest: File) = IO { src.renameTo(dest) }
  def copy(src: File, dest: File) = IOUtils.copyFile(src, dest) 

  def read(src: File) = IOUtils.readFileToString(src) 
  def write(dest: File, content: String) = IOUtils.writeToFile(content, dest)

  def mkdir(dir: File): IO[Boolean] = IO { dir.mkdirs() }
}


// vim: set ts=4 sw=4 et:
