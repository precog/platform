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

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.cache.Cache
import com.precog.common.ingest._
import com.precog.common.json._
import com.precog.common.security._
import com.precog.niflheim._
import com.precog.util._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.table._

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.dispatch.{Await, Future, Promise}
import akka.pattern.pipe
import akka.util.Timeout

import blueeyes.bkka.FutureMonad

import com.weiglewilczek.slf4s.Logging

import org.apache.commons.io.filefilter.FileFilterUtils

import org.joda.time.DateTime

import scalaz._
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.syntax.traverse._

import java.io.{File, FileFilter, IOException}

import scala.collection.mutable

case class FindProjection(path: Path)

case class AccessProjection(path: Path, apiKey: APIKey)

case class FindChildren(path: Path)

case class FindStructure(path: Path)

sealed trait ProjectionUpdate {
  def path: Path
}

case class ProjectionInsert(path: Path, values: Seq[IngestRecord], ownerAccountId: AccountId) extends ProjectionUpdate

case class ProjectionArchive(path: Path, id: EventId) extends ProjectionUpdate

case class ProjectionGetBlock(path: Path, id: Option[Long], columns: Option[Set[ColumnRef]])

class NIHDBProjectionsActor(
    baseDir: File,
    archiveDir: File,
    fileOps: FileOps,
    chef: ActorRef,
    cookThreshold: Int,
    storageTimeout: Timeout,
    accessControl: AccessControl[Future]
    ) extends Actor with Logging {

  override def preStart() = {
    logger.debug("Starting projections actor with base = " + baseDir)
  }

  private final val disallowedPathComponents = Set(".", "..")

  implicit val M: Monad[Future] = new FutureMonad(context.dispatcher)
  /**
    * Computes the stable path for a given descriptor relative to the given base dir
    */
  private def descriptorDir(baseDir: File, path: Path): File = {
    // The path component maps directly to the FS
    // FIXME: escape user-provided components that match NIHDB internal paths
    val prefix = NIHDBActor.escapePath(path).elements.filterNot(disallowedPathComponents)
    new File(baseDir, prefix.mkString(File.separator))
  }

  // Must return a directory
  def ensureBaseDir(path: Path): IO[File] = IO {
    val dir = descriptorDir(baseDir, path)
    if (!dir.exists && !dir.mkdirs()) {
      throw new Exception("Failed to create directory for projection: " + dir)
    }
    dir
  }

  def findBaseDir(path: Path): Option[File] = {
    val dir = descriptorDir(baseDir, path)
    if (NIHDBActor.hasProjection(dir)) Some(dir) else None
  }

  // Must return a directory
  def archiveDir(path: Path): File = {
    descriptorDir(archiveDir, path)
  }

  def archive(path: Path): IO[PrecogUnit] = {
    logger.debug("Archiving " + path)

    findBaseDir(path) match {
      case None =>
        IO { logger.warn("Base dir " + path + " doesn't exist, skipping archive"); PrecogUnit }

      case Some(base) => {
        val archive = archiveDir(path)
        val timeStampedArchive = new File(archive.getParentFile, archive.getName+"-"+System.currentTimeMillis())
        val archiveParent = timeStampedArchive.getParentFile

        IO {
          if (! archiveParent.isDirectory) {
            // Ensure that the parent dir exists
            if (! archiveParent.mkdirs()) {
              throw new IOException("Failed to create archive parent dir for " + timeStampedArchive)
            }
          }

          if (! archiveParent.canWrite) {
            throw new IOException("Invalid permissions on archive directory parent: " + archiveParent)
          }
        }.flatMap { _ =>
          fileOps.rename(base, timeStampedArchive).map { _ =>
            logger.info("Completed archive on " + path); PrecogUnit
          }
        }
      }
    }
  }

  private val pathFileFilter: FileFilter = {
    import FileFilterUtils.{notFileFilter => not, _}
    import NIHDBActor._
    not(or(nameFileFilter(cookedSubdir), nameFileFilter(rawSubdir)))
  }

  def findChildren(path: Path): Future[Set[Path]] = Future {
    val start = descriptorDir(baseDir, path)

    def descendantHasProjection(dir: File): Boolean = {
      if (!NIHDBActor.hasProjection(dir)) {
        dir.listFiles(pathFileFilter) filter (_.isDirectory) exists descendantHasProjection
      } else true
    }

    (start.listFiles(pathFileFilter).toSet filter descendantHasProjection).map {
      d => NIHDBActor.unescapePath(path / Path(d.getName))
    }
  }(context.dispatcher)

  override def postStop() = {
    logger.info("Initiating shutdown of %d projections".format(projections.size))
    Await.result(projections.values.toList.map (_.close).sequence, storageTimeout.duration)
    logger.info("Projection shutdown complete")
  }

  case class ReductionId(blockid: Long, path: Path, reduction: Reduction[_], columns: Set[(CPath, CType)])

  //private val reductionCache = Cache.simple[ReductionId, AnyRef](MaxSize(1024 * 1024))

  private val projections = mutable.Map.empty[Path, NIHDBProjection]

  private def getProjection(path: Path): IO[NIHDBProjection] = {
    // GET THE AUTHORITIES!
    projections.get(path).map(IO(_)).getOrElse {
      ensureBaseDir(path).map { bd =>
        (new NIHDBProjection(bd, path, chef, cookThreshold, context.system, storageTimeout)).tap { proj =>
          projections += (path -> proj)
        }
      }
    }
  }

  def findProjectionWithAPIKey(path: Path, apiKey: APIKey): Future[Option[NIHDBProjection]] = {
    projections.get(path).orElse(findBaseDir(path).map { _ => getProjection(path).unsafePerformIO }) match {
      case Some(proj) =>
        proj.authorities.flatMap { authorities =>
          accessControl.hasCapability(apiKey, Set(ReducePermission(path, authorities.ownerAccountIds)), Some(new DateTime)) map { canAccess =>
            logger.debug("Access for projection at " + path + " = " + canAccess)
            if (canAccess) Some(proj) else None
          }
        }
      case None =>
        logger.warn("Found no projections at " + path)
        Promise.successful(None)(context.dispatcher)
    }
  }

  def receive = {
    // FIXME: Differentiate between "is one there?" and "make it so"
    case FindProjection(path) =>
      sender ! projections.get(path)

    case FindChildren(path) =>
      findChildren(path) pipeTo sender

    case AccessProjection(path, apiKey) =>
      logger.debug("Accessing projection at " + path + " => " + findBaseDir(path))
      findProjectionWithAPIKey(path, apiKey) pipeTo sender

    case ProjectionInsert(path, records, ownerAccountId) =>
      val requestor = sender
      getProjection(path).map {
        _.insert(records, ownerAccountId) map { _ =>
          requestor ! InsertComplete(path)
        }
      }.except {
        case t: Throwable => IO { logger.error("Failure during projection insert", t) }
      }.unsafePerformIO

    case ProjectionArchive(path, _) =>
      val requestor = sender
      logger.debug("Beginning archive of " + path)

      (projections.get(path) match {
        case Some(proj) =>
          logger.debug("Closing projection at " + path)
          proj.close()

        case None => Promise.successful(PrecogUnit)(context.dispatcher)
      }).map { _ =>
        logger.debug("Clearing projection from cache")
        projections -= path

        archive(path).map { _ =>
          requestor ! ArchiveComplete(path)
        }.except {
          case t: Throwable => IO { logger.error("Failure during archive of " + path, t) }
        }.unsafePerformIO

        logger.debug("Processing of archive request complete on " + path)
      }.onFailure {
        case t: Throwable => logger.error("Failure during archive of " + path, t)
      }

    case ProjectionGetBlock(path, id, columns) =>
      val requestor = sender
      try {
        projections.get(path) match {
          case Some(p) => p.getBlockAfter(id, columns).pipeTo(requestor)
          case None => findBaseDir(path).map {
            _ => getProjection(path).unsafePerformIO.getBlockAfter(id, columns) pipeTo requestor
          }
        }
      } catch {
        case t: Throwable => logger.error("Failure during getBlockAfter:", t)
      }
  }
}
