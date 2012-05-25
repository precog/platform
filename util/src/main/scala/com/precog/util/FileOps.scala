package com.precog.util

import java.io._

import scalaz.Validation
import scalaz.effect._

trait FileOps {
  def exists(src: File): Boolean

  def rename(src: File, dest: File): Unit
  def copy(src: File, dest: File): IO[Validation[Throwable, Unit]]

  def read(src: File): IO[Option[String]]
  def write(dest: File, content: String): IO[Validation[Throwable, Unit]]
}

trait FilesystemFileOps extends FileOps {
  def exists(src: File) = src.exists

  def rename(src: File, dest: File) { src.renameTo(dest) }
  def copy(src: File, dest: File) = IOUtils.copyFile(src, dest) 

  def read(src: File) = IOUtils.readFileToString(src) 
  def write(dest: File, content: String) = IOUtils.writeToFile(content, dest)
}


// vim: set ts=4 sw=4 et:
