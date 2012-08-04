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
import java.nio.charset._
import java.nio.channels._
import java.util.Properties

import org.apache.commons.io.FileUtils

import scalaz._
import scalaz.effect.IO

object IOUtils {
  final val UTF8 = "UTF-8"

  val dotDirs = "." :: ".." :: Nil
  
  def isNormalDirectory(f: File) = f.isDirectory && !dotDirs.contains(f.getName) 

  def walkSubdirs(root: File): IO[Seq[File]] = IO {
    if(!root.isDirectory) List.empty else root.listFiles.filter( isNormalDirectory )
  }

  def readFileToString(f: File): IO[String] = IO {
    FileUtils.readFileToString(f, UTF8)
  }

  def readPropertiesFile(s: String): IO[Properties] = readPropertiesFile { new File(s) } 
  
  def readPropertiesFile(f: File): IO[Properties] = IO {
    val props = new Properties
    props.load(new FileReader(f))
    props
  }

  def writeToFile(s: String, f: File): IO[Unit] = IO {
    FileUtils.writeStringToFile(f, s, UTF8)
  }
  
  /** Performs a safe write to the file. Returns true
   * if the file was completely written, false otherwise
   */
  def safeWriteToFile(s: String, f: File): IO[Boolean] = {
    val tmpFile = File.createTempFile(f.getName, ".tmp", f.getParentFile)

    writeToFile(s, tmpFile) flatMap {
      _ => IO(tmpFile.renameTo(f)) // TODO: This is only atomic on POSIX systems
    }
  }

  def recursiveDelete(dir: File): IO[Unit] = IO {
    FileUtils.deleteDirectory(dir)
  }

  // FIXME: Not technically safe. Should use Guava instead
  def createTmpDir(prefix: String): IO[File] = IO {
    val tmp = File.createTempFile(prefix, "tmp")
    tmp.delete
    tmp.mkdirs
    tmp
  }

  def copyFile(src: File, dest: File): IO[Unit] = IO {
    FileUtils.copyFile(src, dest)
  }

}

// vim: set ts=4 sw=4 et:
