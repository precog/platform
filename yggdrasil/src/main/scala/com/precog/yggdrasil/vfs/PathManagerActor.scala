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
//import blueeyes.json.serialization._
import blueeyes.util.Clock

import com.google.common.cache.RemovalCause

import com.precog.common.Path
import com.precog.common.accounts.AccountId
import com.precog.common.jobs._
import com.precog.common.security._
import Permission._
import com.precog.niflheim.NIHDB
import com.precog.util._
import com.precog.util.cache.Cache
import com.precog.yggdrasil.nihdb._

import com.weiglewilczek.slf4s.Logging

import java.io.{IOException, File}
import java.util.UUID

import org.joda.time.DateTime

import scalaz._
import scalaz.NonEmptyList._
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.stream._
import scalaz.std.tuple._
import scalaz.syntax.effect.id._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._

/**
  * An actor that manages resources under a given path. The baseDir is the version
  * subdir for the path.
  */
final class PathManagerActor(path: Path, baseDir: File, versionLog: VersionLog, resources: DefaultResourceBuilder, permissionsFinder: PermissionsFinder[Future], shutdownTimeout: Duration, jobsManager: JobManager[Future], clock: Clock) extends Actor with Logging {
  import ResourceError._

  private[this] implicit def executor: ExecutionContext = context.dispatcher

  private[this] implicit val futureM = new FutureMonad(executor)

  private def versionDir(version: UUID) = new File(baseDir, version.toString)

  private def dirOpt(dir: File) = dir.isDirectory.option(dir)

  private def createVersion(version: UUID, typeName: String): IO[PrecogUnit] = {
    if (versionLog.isCompleted(version)) {
      IO(PrecogUnit)
    } else {
      for {
        _ <- versionLog.addVersion(VersionEntry(version, typeName))
        vDir = versionDir(version)
        makeDir <- dirOpt(vDir) map { _ =>
          // If a dir for this vesion exists currently, we need to wipe it since
          // we're restarting this sequence
          IOUtils.recursiveDelete(vDir) flatMap { _ =>
            IOUtils.makeDirectory(vDir)
          }
        } getOrElse {
          IOUtils.makeDirectory(vDir)
        }
      } yield makeDir
    }
  }

  private def promoteVersion(version: UUID): Future[PrecogUnit] = {
    // we only promote if the requested version is in progress
    if (versionLog.isCompleted(version)) {
      Promise.successful(PrecogUnit)
    } else {
      Future {
        // ERROR: not safe
        versionLog.completeVersion(version) unsafePerformIO
      }
    }
  }

  // Keeps track of the resources for a given version/authority pair
  // TODO: make this an LRU cache
  private[this] var versions = Map[UUID, Resource]()

  private def checkPermissions[A](value: A, writtenBy: Authorities, apiKey: Option[APIKey], permsFor: AccountId => Set[Permission]): Future[Option[A]] = {
    apiKey.map { key =>
      writtenBy.accountIds.toList.traverse { id =>
        permissionsFinder.apiKeyFinder.hasCapability(key, permsFor(id), Some(clock.now))
      } map {
        _.forall(_ == true).option(value)
      }
    } getOrElse {
      Promise.successful(Some(value))
    }
  }

  private def performCreate(apiKey: APIKey, data: PathData, version: UUID, authorities: Authorities): Future[PathActionResponse] = {
    permissionsFinder.checkWriteAuthorities(authorities, apiKey, path, clock.instant) map { okToWrite =>
      if (okToWrite) {
        (for {
        // ERROR: not safe
          _ <- versionLog.addVersion(VersionEntry(version, data.typeName))
          created <- data match {
            case BlobData(bytes, mimeType) =>
              resources.createBlob[IO](versionDir(version), mimeType, authorities, bytes :: StreamT.empty[IO, Array[Byte]])

            case NIHDBData(data) =>
              resources.createNIHDB(versionDir(version), authorities)
          }
          cached <- created traverse {
            // ERROR: not safe
            _ tap { resource => IO { versions += (version -> resource) } }
          }
        } yield {
          created.fold(
            e => UpdateFailure(path, nels(e)),
            _ => UpdateSuccess(path)
          )
        }).unsafePerformIO
      } else {
        UpdateFailure(path, nels(PermissionsError("Key %s does not have permission to write to %s under %d".format(apiKey, path, authorities))))
      }
    }
  }

  private def getNihdb(version: UUID): Future[ValidationNel[ResourceError, NIHDBResource]] = {
    versions.get(version) map {
      case nr: NIHDBResource => Promise successful Success(nr)
      case uhoh => Promise successful Failure(nels(GeneralError("Located resource on %s is a BLOB, not a projection" format path)))
    } orElse {
      versionLog.find(version) map { _ =>
        // ERROR: not safe, and we don't cache the version here...
        Future { resources.openNIHDB(versionDir(version)).unsafePerformIO }
      }
    } getOrElse {
      Promise successful Failure(nels(MissingData("No matching version could be found for " + version)))
    }
  }

  private def getNihdbProjection(version: UUID): Future[ValidationNel[ResourceError, NIHDBProjection]] = {
    getNihdb(version) flatMap { nv =>
      nv traverse { nr =>
        NIHDBProjection.wrap(nr)
      }
    }
  }

  private def getResource(version: UUID): Future[ValidationNel[ResourceError, Resource]] = {
    versions.get(version) map { res =>
      Promise successful Success(res)
    } orElse {
      versionLog.find(version) map { _ =>
        Future {
          (if (NIHDB.hasProjection(versionDir(version))) {
            resources.openNIHDB(versionDir(version))
          } else {
            resources.openBlob(versionDir(version))
          }).unsafePerformIO
        }
      }
    } getOrElse {
      Promise successful Failure(nels(MissingData("No matching version could be found for " + version)))
    }
  }

  private def hasData(version: Option[UUID]): Boolean = {
    (for {
      v <- version
      dir <- dirOpt(versionDir(v))
      files <- Option(dir.list)
    } yield {
      files.size > 0
    }) getOrElse false
  }

  override def postStop = {
    Await.result(versions.values.toStream.traverse(_.close), shutdownTimeout)
  }



  def receive = {
    case CreateNewVersion(_, data, version, apiKey, authorities, overwrite) =>
      if (!overwrite && hasData(versionLog.current.map(_.id))) {
        // TODO: add backchannel error
        sender ! UpdateFailure(path, nels(GeneralError("Non-overwrite create request on existing data")))
      } else {
        performCreate(apiKey, data, version, authorities) recover {
          case t: Throwable =>
            val msg = "Error during data creation " + path
            logger.error(msg, t)
            UpdateFailure(path, nels(GeneralError(msg)))
        } pipeTo sender
      }

    case Append(_, bd: BlobData, _, _, jobIdOpt) =>
      Future {
        val errMsg = "Append not yet supported for blob data"
        logger.error(errMsg)
        UpdateFailure(path, nels(GeneralError(errMsg)))
      }

    case Append(_, data @ NIHDBData(events), key, authorities, jobIdOpt) =>
      versionLog.current map { version =>
        getNihdb(version.id) map { _ map { projection =>
          projection.db.insert(events)
        } fold (
          e => UpdateFailure(path, e),
          _ => UpdateSuccess(path)
        )}
      } getOrElse {
        val version = UUID.randomUUID
        for {
          created <- performCreate(key, data, version, authorities)
          _       <- promoteVersion(version)
        } yield created
      } recover {
        case t: Throwable =>
          val msg = "Failure during append to " + path
          logger.error(msg, t)
          UpdateFailure(path, nels(IOError(t)))
      } pipeTo sender

    case MakeCurrent(_, id, jobId) =>
      promoteVersion(id) map {
        _ => UpdateSuccess(path)
      } recover {
        case t: Throwable =>
          val msg = "Error during promotion of " + id + " to current"
          logger.error(msg, t)
          UpdateFailure(path, nels(IOError(t)))
      } pipeTo sender

    case Read(_, id, auth) =>
      val resVF: Future[ReadResult] = (id orElse versionLog.current.map(_.id)).map { version =>
        getResource(version) flatMap {
          _ traverse { resource =>
          // TODO: should we even be checking perms here?
            checkPermissions(resource, resource.authorities, auth, id => Set(ReadPermission(path, WrittenByAccount(id)))) map { resOpt =>
              ReadSuccess(path, resOpt)
            }
          } map {
            _ valueOr { errors =>
              ReadFailure(path, errors)
            }
          }
        }
      } getOrElse {
        Promise successful ReadSuccess(path, None)
      }

      resVF recover {
        case t: Throwable =>
          val msg = "Error during read on " + id
          logger.error(msg, t)
          ReadFailure(path, nels(IOError(t)))
      } pipeTo sender

    case ReadProjection(_, id, auth) =>
      val projectionVF = (id orElse versionLog.current.map(_.id)) map { version =>
        getNihdbProjection(version) map {
          _ traverse { projection =>
            checkPermissions(projection, projection.authorities, auth, id => Set(ReducePermission(path, WrittenByAccount(id)))) map { projOpt =>
              ReadProjectionSuccess(path, projOpt)
            }
          } map {
            _ valueOr { errors =>
              ReadProjectionFailure(path, errors)
            }
          }
        }
      } getOrElse {
        Promise successful ReadProjectionSuccess(path, None)
      }

      projectionVF recover {
        case t: Throwable =>
          val msg = "Error during read projection on " + id
          logger.error(msg, t)
          ReadProjectionFailure(path, nels(IOError(t)))
      } pipeTo sender

    case Execute(_, auth) =>
      // Return projection snapshot if possible.
    case Stat(_, _) =>
      // Owners, a bit of history, mime type, etc.
  }
}
