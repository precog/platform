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
