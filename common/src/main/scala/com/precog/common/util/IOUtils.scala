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
package com.precog.common.util

import java.io._
import java.nio.charset._
import java.nio.channels._
import java.util.Properties

import scalaz._
import scalaz.effect.IO

object IOUtils {
  val dotDirs = "." :: ".." :: Nil
  
  def isNormalDirectory(f: File) = f.isDirectory && !dotDirs.contains(f.getName) 

  def walkSubdirs(root: File): IO[Seq[File]] =
    IO { if(!root.isDirectory) List.empty else root.listFiles.filter( isNormalDirectory ) }

  def rawReadFileToString(f: File): String = {
    val stream = new FileInputStream(f)
    try {
      val fc = stream.getChannel
      val bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size)
      /* Instead of using default, pass in a decoder. */
      return Charset.defaultCharset().decode(bb).toString
    } finally {
      stream.close
    }
  }

  def readFileToString(f: File): IO[Option[String]] = {

    IO { if(f.exists && f.canRead) Some(rawReadFileToString(f)) else None }
  }

  def readPropertiesFile(s: String): IO[Properties] = readPropertiesFile { new File(s) } 
  
  def readPropertiesFile(f: File): IO[Properties] = IO {
    val props = new Properties
    props.load(new FileReader(f))
    props
  }

  def writeToFile(s: String, f: File): IO[Validation[Throwable, Unit]] = IO {
    Validation.fromTryCatch {
      val writer = new PrintWriter(new PrintWriter(f))
      try {
        writer.println(s)
      } finally { 
        writer.close
      }
    }
  }

  def safeWriteToFile(s: String, f: File): IO[Validation[Throwable, Unit]] = IO {
    Validation.fromTryCatch {
      val tmpFile = File.createTempFile(f.getName, ".tmp", f.getParentFile)
      writeToFile(s, tmpFile).unsafePerformIO
      tmpFile.renameTo(f) // TODO: This is only atomic on POSIX systems
      Success(())
    }
  }

  def recursiveDelete(dir: File) {
    dir.listFiles.foreach {
      case d if d.isDirectory => recursiveDelete(d)
      case f => f.delete()
    }   
    dir.delete()
  }

  def createTmpDir(prefix: String): File = {
    val tmp = File.createTempFile(prefix, "tmp")
    tmp.delete
    tmp.mkdirs
    tmp 
  }

  def copyFile(src: File, dest: File): IO[Validation[Throwable, Unit]] = IO {
    Validation.fromTryCatch { Success(nioCopyFile(src, dest)) }
  }

  private def nioCopyFile(sourceFile: File, destFile: File) {
    if(!destFile.exists) {
     destFile.createNewFile
    }
   
    var source: FileChannel = null
    var destination: FileChannel = null
    try {
      source = new FileInputStream(sourceFile).getChannel
      destination = new FileOutputStream(destFile).getChannel
      destination.transferFrom(source, 0, source.size)
    } finally {
      if(source != null) source.close
      if(destination != null) destination.close
    }
  }
}

// vim: set ts=4 sw=4 et:
