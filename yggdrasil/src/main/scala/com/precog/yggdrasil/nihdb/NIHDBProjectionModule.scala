package com.precog.yggdrasil
package nihdb

import com.precog.yggdrasil.table._
import com.precog.util.FileOps

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.util.Duration

import com.weiglewilczek.slf4s.Logger

import java.io.{File, FileNotFoundException, IOException}
import java.util.concurrent.{Executors,TimeoutException}

import scala.collection.Iterator

import scalaz.NonEmptyList
import scalaz.Validation
import scalaz.effect._
import scalaz.syntax.validation._

trait NIHDBProjectionModuleConfig {
  def maxSliceSize: Int
  def projectionTimeout: Duration
}

trait NIHDBProjectionModule extends RawProjectionModule[Future, Long, Slice] with YggConfigComponent {
  type YggConfig <: NIHDBProjectionModuleConfig
  val pmLogger = Logger("NIHDBProjectionModule")

  def cooker: ActorRef
  def actorSystem: ActorSystem
  def asyncContext: ExecutionContext

  class Projection private[NIHDBProjectionModule] (baseDir: File, descriptor: ProjectionDescriptor)
      extends NIHDBProjection(baseDir, descriptor, cooker, yggConfig.maxSliceSize, actorSystem, yggConfig.projectionTimeout)

  trait ProjectionCompanion extends RawProjectionCompanionLike[Future] {
    def fileOps: FileOps

    private final val disallowedPathComponents = Set(".", "..")
    /**
      * Computes the stable path for a given descriptor relative to the given base dir
      */
    private def descriptorDir(baseDir: File, descriptor: ProjectionDescriptor): File = {
      // The path component maps directly to the FS
      // FIXME: escape user-provided components that match NIHDB internal paths
      val prefix = descriptor.commonPrefix.filterNot(disallowedPathComponents.contains)

      new File(baseDir, prefix.mkString(File.separator))
    }

    // Must return a directory
    def ensureBaseDir(descriptor: ProjectionDescriptor): Future[File] = Future {
      val dir = descriptorDir(baseDir, descriptor)
      if (!dir.exists && !dir.mkdirs()) {
        throw new Exception("Failed to create directory for descriptor: " + descriptor)
      }
      dir
    }

    def findBaseDir(descriptor: ProjectionDescriptor): Option[File] = {
      val dir = descriptorDir(baseDir, descriptor)

      dir.isDirectory.some.map(_ => dir)
    }

    // Must return a directory
    def archiveDir(descriptor: ProjectionDescriptor): Future[Option[File]] = Future {
      val dir = descriptorDir(archiveDir, descriptor)
      if (!dir.exists && !dir.mkdirs()) {
        throw new Exception("Failed to create directory for descriptor: " + descriptor)
      }
      Some(dir)
    }

    def apply(descriptor: ProjectionDescriptor): Future[Projection] = {
      pmLogger.debug("Opening NIHDB projection for " + descriptor)
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
          base    <- Promise.successful(findBaseDir(descriptor))(asyncContext)
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

          Future(fileOps.rename(base, timeStampedArchive).unsafePerformIO)(asyncContext)

        case (Some(base), _) =>
          throw new FileNotFoundException("Could not locate archive dir for projection: " + descriptor)
        case _ =>
          pmLogger.warn("Could not locate base dir for projection: " + descriptor + ", skipping archive"); Promise.successful(false)(asyncContext)
      }
    }
  }
}
