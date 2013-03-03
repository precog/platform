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
import scalaz.std.option._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.traverse._

import java.io.{File, FileFilter, IOException}

import scala.collection.mutable

case class FindProjection(path: Path)

case class AccessProjection(path: Path, apiKey: APIKey)

case class FindChildren(path: Path, apiKey: APIKey)

case class FindStructure(path: Path)

sealed trait ProjectionUpdate {
  def path: Path
}

case class ProjectionInsert(path: Path, values: Seq[IngestRecord], writeAs: Authorities) extends ProjectionUpdate

case class ProjectionArchive(path: Path, archiveBy: APIKey, id: EventId) extends ProjectionUpdate

//case class ProjectionGetBlock(path: Path, id: Option[Long], columns: Option[Set[ColumnRef]])

class NIHDBProjectionsActor(
    activeDir: File,
    archiveDir: File,
    fileOps: FileOps,
    chef: ActorRef,
    cookThreshold: Int,
    storageTimeout: Timeout,
    apiKeyFinder: APIKeyFinder[Future]
    ) extends Actor with Logging {

  implicit val M: Monad[Future] = new FutureMonad(context.dispatcher)

  private final val disallowedPathComponents = Set(".", "..")

  private final val perAuthoritySubdir = "perAuthProjections"

  private final val perAuthoritySubdirEscape = Set(perAuthoritySubdir)

  private final val pathFileFilter: FileFilter = {
    import FileFilterUtils.{notFileFilter => not, _}
    not(nameFileFilter(perAuthoritySubdir))
  }

  private var projections = Map.empty[Path, Map[Authorities, NIHDBActorProjection]]

  case class ReductionId(blockid: Long, path: Path, reduction: Reduction[_], columns: Set[(CPath, CType)])

  override def preStart() = {
    logger.debug("Starting projections actor with base = " + activeDir)
  }

  override def postStop() = {
    logger.info("Initiating shutdown of %d projections".format(projections.size))
    Await.result(projections.values.toList.map (_.values.toList.map(_.close(context.system))).flatten.sequence, storageTimeout.duration)
    logger.info("Projection shutdown complete")
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
  def ensureDescriptorDir(path: Path, authorities: Authorities): IO[File] = IO {
    val dir = descriptorDir(activeDir, path, authorities)
    if (!dir.exists && !dir.mkdirs()) {
      throw new Exception("Failed to create directory for projection: " + dir)
    }
    dir
  }

  def findDescriptorDirs(path: Path): Option[Set[File]] = {
    // No pathFileFilter needed here, since the projections dir should only contain descriptor dirs
    Option(projectionsDir(activeDir, path).listFiles).map {
      _.filter { dir =>
        dir.isDirectory && NIHDBActor.hasProjection(dir)
      }.toSet
    }
  }

  def archive(path: Path, authorities: Authorities): IO[PrecogUnit] = {
    logger.debug("Archiving " + path + " : " + authorities)
    val current = descriptorDir(activeDir, path, authorities)
    if (NIHDBActor.hasProjection(current)) {
      val archive = descriptorDir(archiveDir, path, authorities)
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
        fileOps.rename(current, timeStampedArchive).map { _ =>
          logger.info("Completed archive on " + path); PrecogUnit
        }
      }
    } else {
      IO { logger.warn("Base dir " + path + " doesn't exist, skipping archive"); PrecogUnit }
    }
  }

  def archiveByAPIKey(path: Path, apiKey: APIKey): Future[PrecogUnit] = {
    def deleteOk(apiKey: APIKey, authorities: Authorities): Future[Boolean] =
      apiKeyFinder.hasCapability(apiKey, Set(DeletePermission(path, Permission.WrittenBy(authorities))), Some(new DateTime))

    // First, we need to figure out which projections are actually
    // being archived based on the authorities and perms of the api key
    val toArchive: Future[List[Authorities]] = projections.get(path) match {
      case Some(projections) =>
        projections.toList.map {
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
        }.sequence.map(_.flatten)

      case None =>
        // No open projections, so we need to scan the descriptors
        val authorities = findDescriptorDirs(path).map {
          _.toList.map { dir =>
            NIHDBActor.readDescriptor(dir).map {
              _.map(_.map(_.authorities).valueOr { e => throw new Exception("Error reading projection descriptor for %s from %s: %s".format(path, dir, e.message)) })
            }
          }.sequence.unsafePerformIO.flatten
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

        authorities.map { auth =>
          archive(path, auth).except {
            case t: Throwable => IO { logger.error("Failure during archive of projction %s:%s".format(path, auth), t) }
          }
        }.sequence.unsafePerformIO

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
      permissions <- apiKeyFinder.listPermissions(apiKey, path)
    } yield {
      val writtenBy = permissions collect {
        case perm @ WrittenByPermission(p0, _) if p0.isEqualOrParent(path) => perm
      }

      // FIXME: Not comprehensive/exhaustive in terms of finding all possible data you could read
      val allowedPaths = writtenBy.map(_.path)

      Option(pathDir(activeDir, path).listFiles(pathFileFilter)).map { files =>
        files.filter(_.isDirectory).map { dir => path / Path(dir.getName) }.filter { p => allowedPaths.exists(_.isEqualOrParent(p)) }.toSet
      }.getOrElse(Set.empty)
    }
  }

  private def createProjection(path: Path, authorities: Authorities): IO[NIHDBActorProjection] = {
    projections.get(path).flatMap(_.get(authorities)).map(IO(_)) getOrElse {
      for {
        bd <- ensureDescriptorDir(path, authorities)
        dbv <- NIHDB.create(chef, authorities, bd, cookThreshold, storageTimeout)(context.system)
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
    projections.get(path).map { existing => IO(existing.values.toList) } orElse {
      findDescriptorDirs(path).map { projDirs =>
        projDirs.toList.map { bd =>
          NIHDB.open(chef, bd, cookThreshold, storageTimeout)(context.system).map { dbov =>
            dbov.map { dbv =>
              dbv.map { case (authorities, db) =>
                val proj = new NIHDBActorProjection(db)(context.dispatcher)
                projections += (path -> (projections.getOrElse(path, Map()) + (authorities -> proj)))

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
        inputs.toNel.map { ps =>
          if (ps.size == 1) {
            Promise.successful(ps.head)(context.dispatcher)
          } else {
            ps.map(_.getSnapshot).sequence.map { snaps => new NIHDBAggregateProjection(snaps) }
          }
        }.sequence

      case None => Promise.successful(None)(context.dispatcher)
    }
  }

  def findProjections(path: Path, apiKey: Option[APIKey]): Future[Option[List[NIHDBProjection]]] = {
    import Permission._
    projections.get(path) match {
      case Some(projs) =>
        Promise.successful(Some(projs.values.toList))(context.dispatcher)

      case None =>
        openProjections(path).map(_.unsafePerformIO) match {
          case None =>
            logger.warn("No projections found at " + path)
            Promise.successful(None)(context.dispatcher)

          case Some(foundProjections) =>
            foundProjections.map { proj =>
              apiKey.map { key =>
                proj.authorities.flatMap { authorities =>
                  authorities.ownerAccountIds.toList.toNel map { ids =>
                    apiKeyFinder.hasCapability(key, Set(ReducePermission(path, WrittenBy.oneOf(ids))), Some(new DateTime)) map { canAccess =>
                      logger.debug("Access for projection at " + path + " = " + canAccess)
                      canAccess.option(proj)
                    }
                  } getOrElse {
                    logger.error("Projection " + proj + " at path " + path + " has empty set of authorities.")
                    Promise.successful(None)(context.dispatcher)
                  }
                }
              }.getOrElse {
                Promise.successful(Some(proj))(context.dispatcher)
              }
            }.sequence.map { accessible: List[Option[NIHDBProjection]] => Some(accessible.flatten) }
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

    case ProjectionInsert(path, records, authorities) =>
      val requestor = sender
      createProjection(path, authorities).map {
        _.insert(records) map { _ =>
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

// Not used anywhere
//    case ProjectionGetBlock(path, id, columns) =>
//      val requestor = sender
//      try {
//        projections.get(path) match {
//          case Some(p) => p.getBlockAfter(id, columns).pipeTo(requestor)
//          case None => findBaseDir(path).map {
//            _ => openProjection(path).unsafePerformIO.map(_.getBlockAfter(id, columns)).getOrElse(Promise.successful(None)) pipeTo requestor
//          }
//        }
//      } catch {
//        case t: Throwable => logger.error("Failure during getBlockAfter:", t)
//      }
  }
}
