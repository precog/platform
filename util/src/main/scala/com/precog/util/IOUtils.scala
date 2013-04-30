package com.precog.util

import java.io._
import java.nio.charset._
import java.nio.channels._
import java.util.Properties

import com.google.common.io.Files

import com.weiglewilczek.slf4s.Logging

import org.apache.commons.io.FileUtils

import scalaz._
import scalaz.effect.IO

import scala.collection.JavaConversions.{seqAsJavaList}

object IOUtils extends Logging {
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

  def writeToFile(s: String, f: File, append: Boolean = false): IO[PrecogUnit] = IO {
    FileUtils.writeStringToFile(f, s, UTF8, append)
    PrecogUnit
  }

  def writeSeqToFile[A](s0: Seq[A], f: File): IO[Unit] = IO {
    val s = seqAsJavaList(s0)
    FileUtils.writeLines(f, s)
  }

  /** Performs a safe write to the file. Returns true
   * if the file was completely written, false otherwise
   */
  def safeWriteToFile(s: String, f: File): IO[Boolean] = {
    val tmpFile = new File(f.getParentFile, f.getName + "-" + System.nanoTime + ".tmp")

    writeToFile(s, tmpFile) flatMap {
      _ => IO(tmpFile.renameTo(f)) // TODO: This is only atomic on POSIX systems
    }
  }

  def makeDirectory(dir: File): IO[PrecogUnit] = IO {
    if (dir.isDirectory || dir.mkdirs) PrecogUnit
    else throw new IOException("Failed to create directory " + dir)
  }

  def recursiveDelete(dir: File): IO[PrecogUnit] = IO {
    FileUtils.deleteDirectory(dir)
    PrecogUnit
  }

  /** Recursively deletes empty directories, stopping at the first
    * non-empty dir.
    */
  def recursiveDeleteEmptyDirs(startDir: File, upTo: File): IO[PrecogUnit] = {
    if (startDir == upTo) {
      IO { logger.debug("Stopping recursive clean at root: " + upTo); PrecogUnit }
    } else if (startDir.isDirectory) {
      if (Option(startDir.list).exists(_.length == 0)) {
        IO {
          startDir.delete()
        }.flatMap { _ => recursiveDeleteEmptyDirs(startDir.getParentFile, upTo) }
      } else {
        IO { logger.debug("Stopping recursive clean on non-empty directory: " + startDir); PrecogUnit }
      }
    } else {
      IO { logger.warn("Asked to clean a non-directory: " + startDir); PrecogUnit }
    }
  }

  def createTmpDir(prefix: String): IO[File] = IO {
    val tmpDir = Files.createTempDir()
    Option(tmpDir.getParentFile).map { parent =>
      val newTmpDir = new File(parent, prefix + tmpDir.getName)
      if (! tmpDir.renameTo(newTmpDir)) {
        sys.error("Error on tmpdir creation: rename to prefixed failed")
      }
      newTmpDir
    }.getOrElse { sys.error("Error on tmpdir creation: no parent dir found") }
  }

  def copyFile(src: File, dest: File): IO[PrecogUnit] = IO {
    FileUtils.copyFile(src, dest)
    PrecogUnit
  }

}

// vim: set ts=4 sw=4 et:
