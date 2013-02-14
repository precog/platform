/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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

    // Must return a directory
    def ensureBaseDir(descriptor: ProjectionDescriptor): Future[File]
    def findBaseDir(descriptor: ProjectionDescriptor): Option[File]

    // Must return a directory
    def archiveDir(descriptor: ProjectionDescriptor): Future[Option[File]]

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
