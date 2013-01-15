package com.precog.yggdrasil
package jdbm3

import table._
import com.precog.util.FileOps

import org.joda.time.DateTime

import java.io.{File, FileNotFoundException, IOException}
import java.nio.ByteBuffer
import java.util.concurrent.{Executors,TimeoutException}

import scala.collection.Iterator
import scalaz.NonEmptyList
import scalaz.Validation
import scalaz.effect._
import scalaz.syntax.validation._

import com.weiglewilczek.slf4s.Logger

trait JDBMProjectionModuleConfig {
  def maxSliceSize: Int
}

trait JDBMProjectionModule extends ProjectionModule with YggConfigComponent {
  type YggConfig <: JDBMProjectionModuleConfig
  val pmLogger = Logger("JDBMProjectionModule")

  // type Key = Identities
  type Key = Array[Byte]
  class Projection private[JDBMProjectionModule] (baseDir: File, descriptor: ProjectionDescriptor) extends JDBMProjection(baseDir, descriptor, yggConfig.maxSliceSize)

  trait JDBMProjectionCompanion extends ProjectionCompanion {
    def fileOps: FileOps

    // Must return a directory
    def ensureBaseDir(descriptor: ProjectionDescriptor): IO[File]
    def findBaseDir(descriptor: ProjectionDescriptor): Option[File]
    
    // Must return a directory  
    def archiveDir(descriptor: ProjectionDescriptor): IO[Option[File]]

    def open(descriptor: ProjectionDescriptor): IO[Projection] = {
      pmLogger.debug("Opening JDBM projection for " + descriptor)
      ensureBaseDir(descriptor) map { bd => new Projection(bd, descriptor) }
    }

    def close(projection: Projection) = {
      pmLogger.debug("Requesting close on " + projection)
      projection.close()
    }
    
    def archive(descriptor: ProjectionDescriptor) = {
      pmLogger.debug("Archiving " + descriptor)
      val dirs = 
        for {
          base    <- IO { findBaseDir(descriptor) }
          archive <- archiveDir(descriptor)
        } yield (base, archive) 

      dirs flatMap {
        case (Some(base), Some(archive)) =>
          val timeStampedArchive = new File(archive.getParentFile, archive.getName+"-"+System.currentTimeMillis())
          val archiveParent = timeStampedArchive.getParentFile
          if (! archiveParent.isDirectory) {
            // Ensure that the parent dir exists
            if (! archiveParent.mkdirs()) {
              throw new IOException("Failed to create archive parent dir for " + timeStampedArchive)
            }
          }

          if (! archiveParent.canWrite) {
            throw new IOException("Invalid permissions on archive directory parent: " + archiveParent)
          }

          fileOps.rename(base, timeStampedArchive)
          
        case (Some(base), _) =>
          throw new FileNotFoundException("Could not locate archive dir for projection: " + descriptor)
        case _ =>
          pmLogger.warn("Could not locate base dir for projection: " + descriptor + ", skipping archive"); IO(false)
      }
    }
  }
}
