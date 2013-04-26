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

import akka.actor.Actor
import akka.dispatch.{Await, ExecutionContext, Future, Promise}
import akka.pattern.pipe
import akka.util.Duration

import blueeyes.bkka.FutureMonad
import blueeyes.core.http._
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.util.Clock

import com.google.common.cache.RemovalCause

import com.precog.common.Path
import com.precog.common.accounts.AccountId
import com.precog.common.security._
import com.precog.util.PrecogUnit
import com.precog.util.cache.Cache
import com.precog.yggdrasil.nihdb._

import com.weiglewilczek.slf4s.Logging

import java.io.{IOException, File}
import java.util.UUID

import org.joda.time.DateTime

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.stream._
import scalaz.std.tuple._
import scalaz.syntax.effect.id._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.traverse._

/**
  * An actor that manages resources under a given path. The baseDir is the version
  * subdir for the path.
  */
final class PathManagerActor(path: Path, baseDir: File, resources: DefaultResourceBuilder, permissionsFinder: PermissionsFinder[Future], shutdownTimeout: Duration, jobsManager: JobManager[Future], clock: Clock) extends Actor with Logging {
  import ResourceError._

  private[this] implicit def executor: ExecutionContext = context.dispatcher

  private[this] implicit val futureM = new FutureMonad(executor)

  private[this] val versionLog = new VersionLog(baseDir)

  private def dirIfExists(name: String): Option[File] =
    Option(new File(baseDir, name)).flatMap { dir =>
      dir.isDirectory.option(dir)
    }

  private def versionDir(version: UUID) = dirIfExists(version.toString)

  private def createVersion(version: UUID, typeName: String): IO[PrecogUnit] = {
    if (versionLog.isCompleted(version)) {
      IO(PrecogUnit)
    } else {
      for {
        _ <- versionLog.addVersion(VersionEntry(version, typeName))
        makeDir <- versionDir(version) map { tmpDir =>
          // If a dir for this vesion exists currently, we need to wipe it since
          // we're restarting this sequence
          IOUtils.recursiveDelete(tmpDir) flatMap { _ =>
            IOUtils.makeDirectory(tmpDir)
          }
        } orElse {
            Some(IOUtils.makeDirectory(tmpDir))
          }
        }
    } yield makeDir
  }

  private def promoteVersion(version: UUID): Future[PrecogUnit] = {
    // we only promote if the requested version is in progress
    if (versionLog.isCompleted(version)) {
      Promise.successful(PrecogUnit)
    } else {
      Future {
        versionLog.completeVersion(version) unsafePerformIO
      }
    }
  }

  // Keeps track of the resources for a given version/authority pair
  // TODO: make this an LRU cache
  private[this] var versions = Map[UUID, Resource]()

  // This will either return a cached resource for the
  // version+authorities, or open a new resource, creating it if
  // needed.
  private def createProjection(version: UUID, authorities: Authorities): Future[NIHDBResource] = {
    logger.debug("Creating new projection for " + version)
    versions.get(version) map(Future(_.asInstanceOf[NIHDBResource]))) getOrElse {
      Future {
        resources.createNIHDB(versionDir(version), authorities).unsafePerformIO.map { _.tap { resource =>
          versions += (version -> resource)
        } }.valueOr { e => throw e.exception }
      }
    }
  }

  def findResources(version: VersionEntry, apiKey: Option[APIKey], permsFor: AccountId => Set[Permission]): Future[Validation[ResourceError, Option[Resource]]] = {
    import PathData._
    import Permission._

    val preAuthedResource: IO[Validation[ResourceError, Resource]] =
      versions.get(version.id) map { existing =>
        logger.debug("Found cached resource for version " + version.id)
        IO(Success(existing))
      } getOrElse {
        versionDir(version) map { dir =>
          version match {
            case VersionEntry(id, `BLOB`) =>
              resources.openBlob(dir)

            case VersionEntry(id, `NIHDB`) =>
              resources.openNIHDB(dir)
          } flatMap {
            _ traverse {
              _ tap { resource => IO { versions += (version -> resource) } }
            }
          }
        }
      }

    val result = preAuthedResource map {
      _ traverse { proj =>
        logger.debug("Checking found resource at %s, version %s for access".format(path, version.id))
        apiKey.map { key =>
          // must have at a minimum a reduce permission from each authority
          // TODO: get a Clock in here
          proj.authorities.accountIds.toList.traverse { id =>
            permissionsFinder.apiKeyFinder.hasCapability(key, permsFor(id), Some(clock.now))
          } map {
            _.forall(_ == true).option(proj)
          }
        } getOrElse {
          Promise.successful(Some(proj))
        }
      }
    }

    result.except({ case t: Throwable => IO(Promise.failed(t)) }).unsafePerformIO
  }

  private def performCreate(apiKey: APIKey, data: PathData, version: UUID, authorities: Authorities): Future[PathActionResponse] = {
    permissionsFinder.checkWriteAuthorities(authorities, apiKey, path, clock.now) map { okToWrite =>
      if (okToWrite) {
        for {
          _ <- versionLog.addVersion(VersionEntry(version, data.typeName))
          created <- data match {
            case BlobData(bytes, mimeType) =>
              resources.createBlob[IO](tempVersionDir(version), mimeType, authorities, bytes :: StreamT.empty[IO, Array[Byte]])

            case NIHDBData(data) =>
              resources.createNIHDB(tempVersionDir(version), authorities)
          }
          cached <- created traverse {
            _ tap { resource => IO { versions += (version -> resource) } }
          }
        } yield {
          _.fold(
            e => UpdateFailure(path, e),
            _ => UpdateSuccess(path)
          )
        }
      } else {
        UpdateFailure(path, PermissionsError("Key %s does not have permission to write to %s under %d".format(apiKey, path, authorities)))
      }
    }
  }

  private def performRead(id: Option[UUID], auth: Option[APIKey]): Future[Validation[ResourceError, Option[Resource]]] = {
    import Permission._
    import PathData._

    Future { validateVersion(id) }.flatMap {
      case Some(version) => findResources(version, auth, accountId => Set(ReadPermission(path, WrittenByAccount(accountId))))
      case None =>
        val msg = "No matching version could be found for %s:%s".format(id.getOrElse("*"))
        logger.warn(msg)
        Promise.successful(Failure(MissingData(msg)))
    }
  }

  def validateVersion(requested: Option[UUID]): Option[VersionEntry] = requested.flatMap {
    id => versionLog.find(id) orElse { throw new Exception("Could not locate version " + id) }
  } orElse versionLog.current

  def hasData(version: Option[UUID]): Boolean = {
    for {
      v <- version
      dir <- versionDir(v)
      files <- Option(dir.list)
    } yield {
      files.size > 0
    } getOrElse false
  }

  override def postStop = {
    Await.result(versions.values.flatMap(_.values).toStream.traverse(_.close), shutdownTimeout)
  }



  def receive = {
    case CreateNewVersion(_, apiKey, data, version, authorities, overwrite) =>
      if (!overwrite && hasData(versionLog.current.map(_.id))) {
        // TODO: add backchannel error
        sender ! UpdateFailure(path, GeneralError("Non-overwrite create request on existing data"))
      } else {
        performCreate(apiKey, data, version, authorities) recover {
          case t: Throwable =>
            val msg = "Error during data creation " + path
            logger.error(msg, t)
            UpdateFailure(path, GeneralError(msg))
        } pipeTo sender
      }

    case Append(_, data, writeId, jobIdOpt, authorities) =>
      (data match {
        case bd: BlobData =>
          Future {
            val errMsg = "Append not yet supported for blob data"
            logger.error(errMsg)
            UpdateFailure(path, GeneralError(errMsg))
          }

        case NIHDBData(events) =>
          // Which version are we appending to? If writeId is a UUID,
          // we need to validate that version, otherwise we'll use
          // head
          Future { validateVersion(writeId.toOption).map(_.id) }.flatMap {
            case Some(version) =>
              for {
                projection <- createProjection(version, authorities)
                result <- projection.db.insert(events)
              } yield UpdateSuccess(path)

            case None =>
              val version = UUID.randomUUID

              for {
                created <- performCreate(data, version, Some(authorities))
                _       <- promoteVersion(version)
              } yield created
          }
      }) recover {
        case t: Throwable =>
          val msg = "Failure during append to " + path
          logger.error(msg, t)
          UpdateFailure(path, IOError(t))
      } pipeTo sender

    case Replace(_, id) =>
      Future {
        promoteVersion(id).map({ _ => UpdateSuccess(path)}).unsafePerformIO
      } recover {
        case t: Throwable =>
          val msg = "Error during promotion of " + id
          logger.error(msg, t)
          UpdateFailure(path, IOError(t))
      } pipeTo sender

    case Read(_, id, auth) =>
      performRead(id, auth) map {
        _.fold(
          messages => ReadFailure(path, messages),
          resources => ReadSuccess(path, resources)
        )
      } recover {
        case t: Throwable =>
          val msg = "Error during promotion of " + id
          logger.error(msg, t)
          ReadFailure(path, NEL(IOError(t)))
      } pipeTo sender

    case ReadProjection(_, id, auth) =>
      performRead(id, auth) flatMap(aggregateProjections) map {
        _.fold(
          messages => ReadProjectionFailure(path, messages),
          proj => ReadProjectionSuccess(path, proj)
        )
      } recover {
        case t: Throwable =>
          val msg = "Error during promotion of " + id
          logger.error(msg, t)
          ReadProjectionFailure(path, NEL(IOError(t)))
      } pipeTo sender

    case Execute(_, auth) =>
      // Return projection snapshot if possible.
    case Stat(_, _) =>
      // Owners, a bit of history, mime type, etc.
  }
}
