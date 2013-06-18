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

import com.google.common.util.concurrent.ThreadFactoryBuilder

import com.precog.common._
import com.precog.common.accounts._
import com.precog.util.cache.Cache
import com.precog.common.ingest._
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
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.std.boolean._

import scalaz.syntax.std.list._
import scalaz.syntax.traverse._

import java.io.{File, FileFilter, IOException}
import java.util.concurrent.ScheduledThreadPoolExecutor

import scala.collection.mutable

case class FindProjection(path: Path)

case class AccessProjection(path: Path, apiKey: APIKey)

case class FindChildren(path: Path, apiKey: APIKey)

case class FindStructure(path: Path)

sealed trait ProjectionUpdate {
  def path: Path
}

// Collects a sequnce of sequential batches into a single insert
case class ProjectionInsert(path: Path, batches: Seq[(Long, Seq[IngestRecord])], writeAs: Authorities) extends ProjectionUpdate

case class ProjectionArchive(path: Path, archiveBy: APIKey, id: EventId) extends ProjectionUpdate

//case class ProjectionGetBlock(path: Path, id: Option[Long], columns: Option[Set[ColumnRef]])

object NIHDBProjectionsActor {
  private final val disallowedPathComponents = Set(".", "..")

  private final val perAuthoritySubdir = "perAuthProjections"

  private final val perAuthoritySubdirEscape = Set(perAuthoritySubdir)

  private final val pathFileFilter: FileFilter = {
    import FileFilterUtils.{notFileFilter => not, _}
    not(nameFileFilter(perAuthoritySubdir))
  }

  /**
    * Computes the stable path for a given vfs path relative to the given base dir
    */
  def pathDir(baseDir: File, path: Path): File = {
    // The path component maps directly to the FS
    val prefix = NIHDBActor.escapePath(path, perAuthoritySubdirEscape).elements.filterNot(disallowedPathComponents)
    new File(baseDir, prefix.mkString(File.separator))
  }

  // For a given path, where is the subdir where per-authority projections are stored
  def projectionsDir(baseDir: File, path: Path): File =
    new File(pathDir(baseDir, path), perAuthoritySubdir)

  def descriptorDir(baseDir: File, path: Path, authorities: Authorities): File = {
    //The projections are stored in the perAuthoritySubdir, relative to the path dir, in a folder based on their authority's hash
    new File(projectionsDir(baseDir, path), authorities.sha1)
  }

  // Must return a directory
  def ensureDescriptorDir(activeDir: File, path: Path, authorities: Authorities): IO[File] = IO {
    val dir = descriptorDir(activeDir, path, authorities)
    if (!dir.exists && !dir.mkdirs()) {
      throw new Exception("Failed to create directory for projection: " + dir)
    }
    dir
  }

  def findDescriptorDirs(activeDir: File, path: Path): Option[Set[File]] = {
    // No pathFileFilter needed here, since the projections dir should only contain descriptor dirs
    Option(projectionsDir(activeDir, path).listFiles).map {
      _.filter { dir =>
        dir.isDirectory && NIHDBActor.hasProjection(dir)
      }.toSet
    }
  }
}

class NIHDBProjectionsActor(
    activeDir: File,
    archiveDir: File,
    fileOps: FileOps,
    chef: ActorRef,
    cookThreshold: Int,
    storageTimeout: Timeout,
    permissionsFinder: PermissionsFinder[Future],
    txLogSchedulerSize: Int = 20 // default for now, should come from config in the future
    ) extends Actor with Logging {

  import permissionsFinder.apiKeyFinder
  import NIHDBProjectionsActor._

  implicit val M: Monad[Future] = new FutureMonad(context.dispatcher)

  private var projections = Map.empty[Path, Map[Authorities, NIHDBActorProjection]]

  private final val txLogScheduler = new ScheduledThreadPoolExecutor(txLogSchedulerSize, (new ThreadFactoryBuilder()).setNameFormat("HOWL-sched-%03d").build())

  case class ReductionId(blockid: Long, path: Path, reduction: Reduction[_], columns: Set[(CPath, CType)])

  override def preStart() = {
    logger.debug("Starting projections actor with base = " + activeDir)
  }

  override def postStop() = {
    logger.info("Initiating shutdown of %d projections".format(projections.size))
    Await.result(projections.values.flatMap(_.values).toList.traverse(_.close(context.system)), storageTimeout.duration)
    logger.info("Projection shutdown complete")
    logger.info("Shutting down TX log scheduler")
    txLogScheduler.shutdown()
    logger.info("TX log scheduler shutdown complete")
  }

  def archive(path: Path, authorities: Authorities): IO[PrecogUnit] = {
    logger.debug("Archiving " + path + " : " + authorities)
    val current = descriptorDir(activeDir, path, authorities)
    if (NIHDBActor.hasProjection(current)) {
      val archive = descriptorDir(archiveDir, path, authorities)
      val timeStampedArchive = new File(archive.getParentFile, archive.getName+"-"+System.currentTimeMillis())
      val archiveParent = timeStampedArchive.getParentFile

      logger.debug("Archiving via move of %s to %s".format(current, timeStampedArchive))

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
        fileOps.moveDir(current, timeStampedArchive).map { _ =>
          logger.info("Completed archive on %s (%s => %s)".format(path, current, timeStampedArchive)); PrecogUnit
        }
      }.flatMap { _ =>
        logger.info("Cleaning directories under " + current)
        // Now we need to clean up any empty dirs between here and the root
        IOUtils.recursiveDeleteEmptyDirs(current.getParentFile, activeDir)
      }
    } else {
      IO { logger.warn("Base dir " + path + " doesn't exist, skipping archive"); PrecogUnit }
    }
  }

  def archiveByAPIKey(path: Path, apiKey: APIKey): Future[PrecogUnit] = {
    import Permission._

    def deleteOk(apiKey: APIKey, authorities: Authorities): Future[Boolean] = {
      authorities.accountIds.toList traverse { accountId =>
        // TODO: need a Clock in here
        apiKeyFinder.hasCapability(apiKey, Set(DeletePermission(path, WrittenByAccount(accountId))), Some(new DateTime))
      } map {
        _.exists(_ == true)
      }
    }

    // First, we need to figure out which projections are actually
    // being archived based on the authorities and perms of the api key
    val toArchive: Future[List[Authorities]] = projections.get(path) match {
      case Some(projections) =>
        projections.toList traverse {
          case (authorities, proj) =>
            deleteOk(apiKey, authorities).flatMap { canDelete =>
              logger.debug("Delete allowed for projection at %s:%s with apiKey %s = %s".format(path, authorities, apiKey, canDelete))
              if (canDelete) {
                logger.debug("Closing projection %s:%s".format(path, authorities))
                proj.close(context.system).map(_ => Some(authorities))
              } else {
                Promise.successful(None)(context.dispatcher)
              }
            }
        } map {
          _.flatten
        }

      case None =>
        // No open projections, so we need to scan the descriptors
        val authorities = findDescriptorDirs(activeDir, path).map {
          _.toList traverse { dir =>
            NIHDBActor.readDescriptor(dir).map {
              _.map(_.map(_.authorities).valueOr { e => throw new Exception("Error reading projection descriptor for %s from %s: %s".format(path, dir, e.message)) })
            }
          } map {
            _.flatten
          } unsafePerformIO
        }.getOrElse(Nil)

        Promise.successful(authorities)(context.dispatcher)
      }

    toArchive.map { authorities =>
      if (authorities.nonEmpty) {
        logger.debug("Clearing projections for %s:%s from cache".format(path, authorities))
        projections.get(path).foreach { currentCache =>
          val newCache = currentCache -- authorities
          if (newCache.nonEmpty) {
            projections += (path -> newCache)
          } else {
            projections -= path
          }
        }

        authorities traverse { auth =>
          archive(path, auth).except {
            case t: Throwable => IO { logger.error("Failure during archive of projction %s:%s".format(path, auth), t) }
          }
        } unsafePerformIO

        logger.debug("Processing of archive request complete on " + path)
      } else {
        logger.debug("No projections found to archive for %s with %s".format(path, apiKey))
      }

      PrecogUnit
    }
  }

  def findChildren(path: Path, apiKey: APIKey): Future[Set[Path]] = {
    implicit val ctx = context.dispatcher
    for {
      allowedPaths <- permissionsFinder.findBrowsableChildren(apiKey, path)
    } yield {
      val pathRoot = pathDir(activeDir, path)

      logger.debug("Checking for children of path %s in dir %s among %s".format(path, pathRoot, allowedPaths))
      Option(pathRoot.listFiles(pathFileFilter)).map { files =>
        logger.debug("Filtering children %s in path %s".format(files.mkString("[", ", ", "]"), path))
        files.filter(_.isDirectory).map { dir => path / Path(dir.getName) }.filter { p => allowedPaths.exists(_.isEqualOrParent(p)) }.toSet
      } getOrElse {
        logger.debug("Path dir %s for path %s is not a directory!".format(pathRoot, path))
        Set.empty
      }
    }
  }

  private def createProjection(path: Path, authorities: Authorities): IO[NIHDBActorProjection] = {
    projections.get(path).flatMap(_.get(authorities)).map(IO(_)) getOrElse {
      for {
        bd <- ensureDescriptorDir(activeDir, path, authorities)
        dbv <- NIHDB.create(chef, authorities, bd, cookThreshold, storageTimeout, txLogScheduler)(context.system)
      } yield {
        val proj = dbv map { db => new NIHDBActorProjection(db)(context.dispatcher) } valueOr { error =>
          sys.error("An error occurred in deserialization of projection metadata for " + path + ", " + authorities + ": " + error.message)
        }

        projections += (path -> (projections.getOrElse(path, Map()) + (authorities -> proj)))

        proj
      }
    }
  }

  private def openProjections(path: Path): Option[IO[List[NIHDBProjection]]] = {
    projections.get(path).map { existing =>
      logger.debug("Found cached projection for " + path)
      IO(existing.values.toList)
    } orElse {
      logger.debug("Opening new projection for " + path)
      findDescriptorDirs(activeDir, path).map { projDirs =>
        projDirs.toList.map { bd =>
          NIHDB.open(chef, bd, cookThreshold, storageTimeout, txLogScheduler)(context.system).map { dbov =>
            dbov.map { dbv =>
              dbv.map { case (authorities, db) =>
                val proj = new NIHDBActorProjection(db)(context.dispatcher)
                projections += (path -> (projections.getOrElse(path, Map()) + (authorities -> proj)))

                logger.debug("Cache for %s updated to %s".format(path, projections.get(path)))

                proj
              } valueOr { error =>
                sys.error("An error occurred in deserialization of projection metadata for " + path + ": " + error.message)
              }
            }
          }
        }.sequence.map(_.flatten)
      }
    }
  }

  def aggregateProjections(path: Path, apiKey: Option[APIKey]): Future[Option[NIHDBProjection]] = {
    findProjections(path, apiKey).flatMap {
      case Some(inputs) =>
        inputs.toNel traverse{ ps =>
          if (ps.size == 1) {
            logger.debug("Aggregate on %s with key %s returns a single projection".format(path, apiKey))
            Promise.successful(ps.head)(context.dispatcher)
          } else {
            logger.debug("Aggregate on %s with key %s returns an aggregate projection".format(path, apiKey))
            ps.traverse(_.getSnapshot) map { snaps => new NIHDBAggregateProjection(snaps) }
          }
        }

      case None =>
        logger.debug("No projections found for " + path)
        Promise.successful(None)(context.dispatcher)
    }
  }

  def findProjections(path: Path, apiKey: Option[APIKey]): Future[Option[List[NIHDBProjection]]] = {
    import Permission._
    openProjections(path).map(_.unsafePerformIO) match {
      case None =>
        logger.warn("No projections found at " + path)
        Promise.successful(None)(context.dispatcher)

      case Some(foundProjections) =>
        logger.debug("Checking found projections at " + path + " for access")
        foundProjections traverse { proj =>
          apiKey.map { key =>
            proj.authorities.flatMap { authorities =>
              // must have at a minimum a reduce permission from each authority
              // TODO: get a Clock in here
              authorities.accountIds.toList traverse { id =>
                apiKeyFinder.hasCapability(key, Set(ReducePermission(path, WrittenByAccount(id))), Some(new DateTime))
              } map {
                _.forall(_ == true).option(proj)
              }
            }
          } getOrElse {
            Promise.successful(Some(proj))(context.dispatcher)
          }
        } map { 
          _.sequence
        }
    }
  }

  // FIXME or OK? Current semantics of returning a "Projection" means that someone could be holding onto a projection that is later archived out from under them
  def receive = {
    // FIXME: Differentiate between "is one there?" and "make it so"
    case FindProjection(path) =>
      aggregateProjections(path, None) pipeTo sender

    case FindChildren(path, apiKey) =>
      findChildren(path, apiKey) pipeTo sender

    case AccessProjection(path, apiKey) =>
      logger.debug("Accessing projections at " + path + " => " + projectionsDir(activeDir, path))
      aggregateProjections(path, Some(apiKey)) pipeTo sender

    case ProjectionInsert(path, batches, authorities) =>
      val requestor = sender
      createProjection(path, authorities).map {
        _.insert(batches) map { _ =>
          requestor ! InsertComplete(path)
        }
      }.except {
        case t: Throwable => IO { logger.error("Failure during projection insert", t) }
      }.unsafePerformIO

    case ProjectionArchive(path, apiKey, _) =>
      val requestor = sender
      logger.debug("Beginning archive of " + path)

      archiveByAPIKey(path, apiKey).map { _ =>
        requestor ! ArchiveComplete(path)
      }.onFailure {
        case t: Throwable => logger.error("Failure during archive of " + path, t)
      }
  }
}
