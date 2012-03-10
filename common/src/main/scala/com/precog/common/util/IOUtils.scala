package com.precog.common.util

import java.io._
import java.util.Properties

import scalaz._
import scalaz.effect.IO

object IOUtils {
  val dotDirs = "." :: ".." :: Nil
  
  def isNormalDirectory(f: File) = f.isDirectory && !dotDirs.contains(f.getName) 

  def walkSubdirs(root: File): IO[Seq[File]] =
    IO { if(!root.isDirectory) List.empty else root.listFiles.filter( isNormalDirectory ) }

  def readFileToString(f: File): IO[Option[String]] = {
    def readFile(f: File): String = {
      val in = scala.io.Source.fromFile(f)
      val content = in.mkString
      in.close
      content
    }
    IO { if(f.exists && f.canRead) Some(readFile(f)) else None }
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

  def createTmpDir(prefix: String) {
    val tmp = File.createTempFile(prefix, "tmp")
    tmp.delete
    tmp.mkdirs
    tmp 
  }

}

// vim: set ts=4 sw=4 et:
