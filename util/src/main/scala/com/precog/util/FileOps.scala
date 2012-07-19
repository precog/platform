package com.precog.util

import java.io._

import scalaz.Validation
import scalaz.effect._

trait FileOps {
  def exists(src: File): IO[Boolean]

  def rename(src: File, dest: File): IO[Boolean]
  def copy(src: File, dest: File): IO[Unit]

  def read(src: File): IO[String]
  def write(dest: File, content: String): IO[Unit]

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
