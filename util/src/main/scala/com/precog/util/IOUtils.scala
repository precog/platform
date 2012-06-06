package com.precog.util

import java.io._
import java.nio.charset._
import java.nio.channels._
import java.util.Properties

import org.apache.commons.io.FileUtils

import scalaz._
import scalaz.effect.IO

object IOUtils {
  val dotDirs = "." :: ".." :: Nil
  
  def isNormalDirectory(f: File) = f.isDirectory && !dotDirs.contains(f.getName) 

  def walkSubdirs(root: File): IO[Seq[File]] = IO {
    if(!root.isDirectory) List.empty else root.listFiles.filter( isNormalDirectory )
  }

  def readFileToString(f: File): IO[String] = IO {
    FileUtils.readFileToString(f, "UTF-8")
  }

  def readPropertiesFile(s: String): IO[Properties] = readPropertiesFile { new File(s) } 
  
  def readPropertiesFile(f: File): IO[Properties] = IO {
    val props = new Properties
    props.load(new FileReader(f))
    props
  }

  def writeToFile(s: String, f: File): IO[Unit] = IO {
    val writer = new PrintWriter(new PrintWriter(f))
    try {
      writer.println(s)
    } finally { 
      writer.close
    }
  }

  def safeWriteToFile(s: String, f: File): IO[Unit] = IO {
    val tmpFile = File.createTempFile(f.getName, ".tmp", f.getParentFile)
    writeToFile(s, tmpFile).unsafePerformIO
    tmpFile.renameTo(f) // TODO: This is only atomic on POSIX systems
    Success(())
  }

  def recursiveDelete(dir: File): IO[Unit] = IO {
    dir.listFiles.foreach {
      case d if d.isDirectory => recursiveDelete(d)
      case f => f.delete()
    }   
    dir.delete()
  }

  def createTmpDir(prefix: String): IO[File] = IO {
    val tmp = File.createTempFile(prefix, "tmp")
    tmp.delete
    tmp.mkdirs
    tmp
  }

  def copyFile(src: File, dest: File): IO[Unit] = IO {
    Success(nioCopyFile(src, dest))
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
