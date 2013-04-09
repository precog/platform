package com.precog.util

import com.google.common.io.Files

import java.io._

import scalaz.Validation
import scalaz.effect._

trait FileOps {
  def exists(src: File): IO[Boolean]

  def rename(src: File, dest: File): IO[Boolean]
  def moveDir(src: File, dest: File): IO[Boolean]

  def copy(src: File, dest: File): IO[PrecogUnit]

  def read(src: File): IO[String]
  def write(dest: File, content: String): IO[PrecogUnit]

  def mkdir(dir: File): IO[Boolean]
}

object FilesystemFileOps extends FileOps {
  def exists(src: File) = IO { src.exists() }

  def rename(src: File, dest: File) = IO { src.renameTo(dest) }
  def moveDir(src: File, dest: File) = IO {
    if (!src.isDirectory) {
      throw new IOException("Source for moveDir (%s) is not a directory".format(src))
    }

    if (!dest.getParentFile.isDirectory) {
      throw new IOException("Destination parent for moveDir (%s) is not a directory".format(dest))
    }

    Files.move(src, dest)

    true
  }

  def copy(src: File, dest: File) = IOUtils.copyFile(src, dest)

  def read(src: File) = IOUtils.readFileToString(src)
  def write(dest: File, content: String) = IOUtils.writeToFile(content, dest)

  def mkdir(dir: File): IO[Boolean] = IO { dir.mkdirs() }
}


// vim: set ts=4 sw=4 et:
