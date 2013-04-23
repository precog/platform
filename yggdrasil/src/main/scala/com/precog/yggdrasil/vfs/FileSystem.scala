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
package com.precog
package yggdrasil
package vfs

import com.google.common.cache.RemovalCause

import com.precog.common.json._

import blueeyes.core.http._
import blueeyes.json._
import blueeyes.json.serialization._

import java.util.UUID

import org.joda.time._

import shapeless._
import scalaz._

sealed class PathData(val typeName: String)
object PathData {
  final val BLOB = "blob"
  final val NIHDB = "nihdb"
}

case class BlobData(data: Array[Byte], mimeType: MimeType) extends PathData(PathData.BLOB)
case class NIHDBData(data: Seq[(Long, Seq[JValue])]) extends PathData(PathData.NIHDB)

sealed trait PathOp

/**
  * Creates a new resource with the given tracking id.
  */
case class Create(data: PathData, streamId: UUID, authorities: Authorities, overwrite: Boolean) extends PathOp

/**
  * Appends data to a resource. If the stream ID is specified, a
  * sequence of Appends must eventually be followed by a Replace.
  */
case class Append(data: PathData, streamId: Option[UUID], authorities: Authorities) extends PathOp

/**
  * Replace the current HEAD with the version specified by the streamId.
  */
case class Replace(streamId: UUID) extends PathOp

case class Read(auth: Option[APIKey]) extends PathOp
case class Execute(auth: Option[APIKey]) extends PathOp
case class Stat(auth: Option[APIKey]) extends PathOp

/**
  * An actor that manages resources under a given path. The baseDir is the version
  * subdir for the path.
  */
final class PathManagerActor(baseDir: File, path: Path, resources: Resources) extends Actor {
  private[this] versionLog = new VersionLog(baseDir)

  private def tempVersionDir(id: UUID) = new File(baseDir, id.toString + "-temp")
  private def mainVersionDir(id: UUID) = new File(baseDir, id.toString)

  private def versionSubdir(id: UUID): OptionFile = {
    if (mainVersionDir(version).isDirectory) mainVersionDir(version) else tempVersionDir(version)
  }

  def createVersion(version: UUID, typeName: String): IO[PrecogUnit] = {
    versionLog.addVersion(VersionEntry(version, typeName)).map { _ =>
      val tmpDir = tempVersionDir(version)
      if (!tmpDir.isDirectory && !tmpDir.mkdirs()) {
        throw new IOException("Failed to create temp dir " + tmpDir)
      }
      PrecogUnit
    }
  }

  def promoteVersion(version: UUID): IO[PrecogUnit] = {
    versionLog.setHead(version).map { _ =>
      if (! mainVersionDir(version).isDirectory && tempVersionDir(version).isDirectory) {
        if (!tempVersionDir.(version).renameTo(mainVersionDir(version))) {
          throw new IOException("Failed to migrate temp dir to main for version " + version)
        }
      }
      PrecogUnit
    }
  }

  // Keeps track of the resources for a given version/authority pair
  private val resources = Cache.simple[UUID, Map[Authorities, Resource]](
    OnRemoval {
      case (_, resourceMap, cause) if cause != RemovalCause.REPLACED => resourceMap.foreach(_.close)
    }) //,
       //ExpireAfterAccess(Duration(30, TimeUnit.Minutes))

  // This will either return a cached resource for the
  // version+authorities, or open a new resource, creating it if
  // needed.
  private def createProjection(version: UUID, authorities: Authorities): Future[NIHDBResource] = {
    resources.get(version).flatMap(_.get(authorities)).map(Promise.successful(_)) getOrElse {
      val projDir = 

      Future {
        resources.createNIHDB(projDir, authorities).unsafePerformIO.map { resource =>
          resources += (version -> (resources.getOrElse(authorities, Map()) + (authorities -> resource)))
          resource
        } valueOr(throw)
      }
    }
  }

  private def openProjections(version: UUID): Option[IO[List[NIHDBResource]]] = {
    resources.get(version).map { existing =>
      logger.debug("Found cached projections for v" + version)
      IO(existing.values.toList.collect { case nr: NIHDBResource => nr })
    } orElse {
      logger.debug("Opening new projections for v" + version)
      resources.findDescriptorDirs(versionSubdir(version)).map { projDirs =>
        projDirs.toList.map { bd =>
          resources.openNIHDB(bd).map { dbv =>
            dbv.map { nihdbResource =>
              resources += (version.current -> (resources.getOrElse(version, Map()) + (nihdbResource.authorities -> nihdbResource)))
              logger.debug("Cache for v%d updated to %s".format(version, resources.get(version)))
              nihdbResource
            }
          }.valueOr { error => sys.error("An error occurred opening the NIHDB projection in %s: %s".format(bd, error.message)) }
        }.sequence.map(_.flatten)
      }
    }
  }

  def aggregateProjections(version: UUID, apiKey: Option[APIKey]): Future[Option[NIHDBResource]] = {
    findProjections(version, apiKey).flatMap {
      case Some(inputs) =>
        inputs.toNel traverse { ps =>
          if (ps.size == 1) {
            logger.debug("Aggregate on v%d with key %s returns a single projection".format(version, apiKey))
            Promise.successful(ps.head)(context.dispatcher)
          } else {
            logger.debug("Aggregate on v%d with key %s returns an aggregate projection".format(version, apiKey))
            for {
              authorities <- ps.traverse(_.authorities)
              snaps <- ps.traverse(_.getSnapshot)
            } yield {
              new NIHDBResource(new NIHDBAggregate(authorities, snaps))
            }
          }
        }

      case None =>
        logger.debug("No projections found for " + path)
        Promise.successful(None)(context.dispatcher)
    }
  }

  def findProjections(version: UUID, apiKey: Option[APIKey]): Future[Option[List[NIHDBProjection]]] = {
    import Permission._
    openProjections(version).map(_.unsafePerformIO) match {
      case None =>
        logger.warn("No projections found at " + path)
        Promise.successful(None)(context.dispatcher)

      case Some(foundProjections) =>
        logger.debug("Checking found projections on v" + version + " for access")
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

  private def performCreate(data: PathData, id: UUID, authorities: Authorities): IO[Validation[ResourceError, Resource]] = {
    for {
      version <- IO { versionLog.addVersion(VersionEntry(id, data.typeName)) }
      result <- data match {
        case BlobData(bytes, mimeType) =>
          resources.createBlob[IO](tempVersionDir(id), mimeType, authorities, bytes :: StreamT.empty)

        case NIHDBData(data) =>
          resources.createNIHDB(tempVersionDir(id), authorities)
      }
      _ = result.map { resource =>
        resources += (version -> Map(authorities -> resource))
      }
    } yield result
  }

  def hasData(version: Option[UUID]): Boolean = {
    version.flatMap({ v => Option(mainVersionDir(v).list).map(_.length) }).exist(_ > 0)
  }


  def receive = {
    case Create(data, id, authorities, overwrite) =>
      if (!overwrite && hasData(versionLog.current)) {
        sender ! CreateFailure("Non-overwrite request of existing data")
      } else {
        val requestor = sender

        performCreate(data, id, authorities).map {
          case Success(_) =>
            requestor ! CreateSuccess(path)

          case Failure(error) =>
            requestor ! CreateFailure(path, error)
        }.except {
          case t: Throwable => IO { logger.error("Error during data creation", t) }
        }.unsafePerformIO
      }

    case Append(data, idOpt, authorities) =>
      val requestor = sender
      val result : Future[PrecogUnit] = data match {
        case BlobData(_, _, id) =>
          Future { sys.error("Append not yet supported for blob data") }

        case NIHDBData(data) =>
          // Which version are we appending to? In order, prefer the
          // provided version, then the current version. If no version
          // is available, we'll create a new one (legacy ingest
          // handling)
          (idOpt orElse versionLog.current) match {
            case Some(version) =>
              for {
                projection <- createProjection(version, authorities)
                result <- projection.insert(data)
              } yield result

            case None =>
              val version = UUID.randomUUID

              Future {
                (for {
                  _ <- createVersion(version)
                  _ <- promoteVersion(version)
                  created <- performCreate(data, version, authorities)
                }).unsafePerformIO.valueOr(throw)
              }
          }
      }).onFailure {
        case t: Throwable =>
          logger.error("Failure during append to " + path, t)
          requestor ! AppendFailure(path, t)
      }.onSuccess {
        _ => requestor ! AppendSuccess(path)
      }

    case Replace(id) =>


    case Read(auth) =>
      // Returns a stream of bytes + mimetype
    case Archive(auth) =>
      rollOver(identity)
    case Execute(auth) =>
      // Return projection snapshot if possible.
    case Stat(_) =>
      // Owners, a bit of history, mime type, etc.
  }
}
