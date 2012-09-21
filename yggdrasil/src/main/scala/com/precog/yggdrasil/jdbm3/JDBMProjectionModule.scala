package com.precog.yggdrasil
package jdbm3

import table._
import com.precog.util.FileOps

import org.joda.time.DateTime

import java.io.{File, FileNotFoundException}
import java.nio.ByteBuffer
import java.util.concurrent.{Executors,TimeoutException}

import scala.collection.Iterator
import scalaz.NonEmptyList
import scalaz.Validation
import scalaz.effect._
import scalaz.syntax.validation._

import com.weiglewilczek.slf4s.Logger

trait JDBMProjectionModule extends ProjectionModule {
  val pmLogger = Logger("JDBMProjectionModule")

  // type Key = Identities
  type Key = Array[Byte]
  class Projection private[JDBMProjectionModule] (baseDir: File, descriptor: ProjectionDescriptor) extends JDBMProjection(baseDir, descriptor)

  trait JDBMProjectionCompanion extends ProjectionCompanion {
    def fileOps: FileOps

    // Must return a directory
    def baseDir(descriptor: ProjectionDescriptor): IO[Option[File]]
    
    // Must return a directory  
    def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]]

    def open(descriptor: ProjectionDescriptor): IO[Projection] = {
      pmLogger.debug("Opening JDBM projection for " + descriptor)
      baseDir(descriptor) map { 
        case Some(bd) => new Projection(bd, descriptor) 
        case None => throw new FileNotFoundException("Could not locate base for projection: " + descriptor)
      }
    }

    def close(projection: Projection) = {
      pmLogger.debug("Requesting close on " + projection)
      IO(projection.close())
    }
    
    def archive(projection: Projection) = {
      pmLogger.debug("Archiving " + projection)
      val descriptor = projection.descriptor
      val dirs = 
        for {
          _       <- close(projection)
          base    <- baseDir(descriptor)
          archive <- archiveDir(descriptor)
        } yield (base, archive) 

      dirs flatMap {
        case (Some(base), Some(archive)) =>
          val timeStampedArchive = new File(archive.getParentFile, archive.getName+"-"+System.currentTimeMillis()) 
          fileOps.rename(base, timeStampedArchive)
          
        case (Some(base), _) =>
          throw new FileNotFoundException("Could not locate archive dir for projection: " + projection)
        case _ =>
          throw new FileNotFoundException("Could not locate base dir for projection: " + projection)
      }
    }
  }
}
