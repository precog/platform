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
import scalaz.std.stream._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._

sealed class PathData(val typeName: String)
object PathData {
  final val BLOB = "blob"
  final val NIHDB = "nihdb"
}

case class BlobData(data: Array[Byte], mimeType: MimeType) extends PathData(PathData.BLOB)
case class NIHDBData(data: Seq[(Long, Seq[JValue])]) extends PathData(PathData.NIHDB)

object NIHDBData {
  val Empty = NIHDBData(Seq.empty)
}

sealed trait PathOp

sealed trait PathUpdateOp extends PathOp

/**
  * Creates a new resource with the given tracking id.
  */
case class Create(path: Path, data: PathData, streamId: UUID, authorities: Option[Authorities], overwrite: Boolean) extends PathUpdateOp

/**
  * Appends data to a resource. If the stream ID is specified, a
  * sequence of Appends must eventually be followed by a Replace.
  */
case class Append(path: Path, data: PathData, streamId: Option[UUID], authorities: Authorities) extends PathUpdateOp

/**
  * Replace the current HEAD with the version specified by the streamId.
  */
case class Replace(path: Path, streamId: UUID) extends PathUpdateOp

case class Read(path: Path, streamId: Option[UUID], auth: Option[APIKey]) extends PathOp
case class ReadProjection(path: Path, streamId: Option[UUID], auth: Option[APIKey]) extends PathOp
case class Execute(path: Path, auth: Option[APIKey]) extends PathOp
case class Stat(path: Path, auth: Option[APIKey]) extends PathOp

case class FindChildren(path: Path, auth: APIKey) extends PathOp

sealed trait PathActionResponse

case class UpdateSuccess(path: Path) extends PathActionResponse
case class UpdateFailure(path: Path, error: ResourceError) extends PathActionResponse

sealed trait ReadResult extends PathActionResponse {
  def resources: List[Resource]
}

case class ReadSuccess(path: Path, resources: List[Resource]) extends ReadResult
case class ReadFailure(path: Path, errors: NEL[ResourceError]) extends ReadResult {
  val resources = Nil
}

sealed trait ReadProjectionResult extends PathActionResponse {
  def projection: Option[NIHDBProjection]
}

case class ReadProjectionSuccess(path: Path, projection: Option[NIHDBProjection]) extends ReadProjectionResult
case class ReadProjectionFailure(path: Path, messages: NEL[ResourceError]) extends ReadProjectionResult {
  val projection = None
}

/**
  * An actor that manages resources under a given path. The baseDir is the version
  * subdir for the path.
  */
final class PathManagerActor(baseDir: File, path: Path, resources: DefaultResourceBuilder, permissionsFinder: PermissionsFinder[Future], shutdownTimeout: Duration) extends Actor with Logging {
  import ResourceError._

  private[this] implicit def executor: ExecutionContext = context.dispatcher

  private[this] implicit val futureM = new FutureMonad(executor)

  private[this] val versionLog = new VersionLog(baseDir)

  private def tempVersionDir(version: UUID) = new File(baseDir, version.toString + "-temp")
  private def mainVersionDir(version: UUID) = new File(baseDir, version.toString)

  private def versionSubdir(version: UUID): File = {
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
        if (! tempVersionDir(version).renameTo(mainVersionDir(version))) {
          throw new IOException("Failed to migrate temp dir to main for version " + version)
        }
      }
      PrecogUnit
    }
  }

  // Keeps track of the resources for a given version/authority pair
  // TODO: make this an LRU cache
  private[this] var versions = Map[UUID, Map[Authorities, Resource]]()

  // This will either return a cached resource for the
  // version+authorities, or open a new resource, creating it if
  // needed.
  private def createProjection(version: UUID, authorities: Authorities): Future[NIHDBResource] = {
    versions.get(version).flatMap(_.get(authorities).map(_.asInstanceOf[NIHDBResource])).map(Promise.successful) getOrElse {
      Future {
        resources.createNIHDB(versionSubdir(version), authorities).unsafePerformIO.map { resource =>
          versions += (version -> (versions.getOrElse(version, Map()) + (authorities -> resource)))
          resource
        }.valueOr { e => throw e.exception }
      }
    }
  }

  private def openProjections(version: UUID): IO[ValidationNel[ResourceError, List[NIHDBResource]]] = {
    logger.debug("Opening new projections for v" + version)
    for {
      projDirs <- resources.findDescriptorDirs(versionSubdir(version))
      opened <- projDirs traverse { bd =>
        resources.openNIHDB(bd) map { proj =>
          proj.foreach { nihdbResource =>
            versions += (version -> (versions.getOrElse(version, Map()) + (nihdbResource.authorities -> nihdbResource)))
            logger.debug("Cache for v%d updated to %s".format(version, versions.get(version)))
          }
          proj.toValidationNel
        }
      }
    } yield {
      opened.sequence[({type λ[α] = ValidationNel[ResourceError, α]})#λ, NIHDBResource]
    }
  }

  def aggregateProjections(found: ValidationNel[ResourceError, List[Resource]]): Future[ValidationNel[ResourceError, Option[NIHDBProjection]]] = {
    found.flatMap { inputs =>
      val nihdbs = inputs.collect({ case n: NIHDBResource => n })

      if (nihdbs.size != inputs.size) {
        Failure(NEL(GeneralError("Resources found, but they are not projections")))
      } else {
        Success(nihdbs)
      }
    } traverse {
      case Nil =>
        logger.debug("No projections found for " + path)
        Promise.successful(None)

      case input :: Nil =>
        logger.debug("Aggregate on %s returns a single projection".format(path))
        NIHDBProjection.wrap(input.db, input.authorities).map(Some(_))

      case inputs =>
        logger.debug("Aggregate on %s returns an aggregate projection".format(path))
        inputs.toNel traverse { inputsNel: NEL[NIHDBResource] =>
          inputsNel.traverse { r => r.db.getSnapshot map { (r.authorities, _) } } map { toAggregate =>
            val (allAuthorities, snapshots) = toAggregate.unzip
            val newAuthorities = allAuthorities.list.reduce(_ |+| _)
              (new NIHDBAggregate(snapshots, newAuthorities)).projection
          }
        }
    }
  }

  def findResources(version: VersionEntry, apiKey: Option[APIKey], permsFor: AccountId => Set[Permission]): Future[ValidationNel[ResourceError, List[Resource]]] = {
    import PathData._
    import Permission._

    val preAuthedResources: IO[ValidationNel[ResourceError, List[Resource]]] =
      versions.get(version.id) map { existing =>
        logger.debug("Found cached projections for version " + version.id)
        IO(Success(existing.values.toList))
      } getOrElse {
        version match {
          case VersionEntry(id, `BLOB`) =>
            resources.openBlob(versionSubdir(id)).map(_.map(List(_)).toValidationNel)

          case VersionEntry(id, `NIHDB`) =>
            openProjections(id)
        }
      }

    val result = preAuthedResources map {
      _ traverse { foundProjections =>
        logger.debug("Checking found resources at %s, version %s for access".format(path, version.id))
        // WANTED: Traverse[Future]
        foundProjections.traverse {
          proj =>
            apiKey.map { key =>
              // must have at a minimum a reduce permission from each authority
              // TODO: get a Clock in here
              proj.authorities.accountIds.toList.traverse { id =>
                permissionsFinder.apiKeyFinder.hasCapability(key, permsFor(id), Some(new DateTime))
              } map {
                _.forall(_ == true).option(proj)
              }
            } getOrElse {
              Promise.successful(Some(proj))
            }
        } map {
          _.flatten
        }
      }
    }

    result.except({ case t: Throwable => IO(Promise.failed(t)) }).unsafePerformIO
  }

  private def performCreate(data: PathData, version: UUID, authorities: Option[Authorities]): IO[Option[Validation[ResourceError, Resource]]] = {
    for {
      _ <- versionLog.addVersion(VersionEntry(version, data.typeName))
      result <- data match {
        case BlobData(bytes, mimeType) => authorities match {
          case Some(auth) => resources.createBlob[IO](tempVersionDir(version), mimeType, auth, bytes :: StreamT.empty[IO, Array[Byte]]).map(Some(_))
          case None => IO(Some(Failure(GeneralError("Cannot create a Blob with empty authorities"))))
        }

        case NIHDBData(data) => authorities traverse { auth =>
          resources.createNIHDB(tempVersionDir(version), auth)
        }
      }
      _ = result.map { _.map { resource: Resource =>
        authorities.foreach { auth =>
          versions += (version -> Map(auth -> resource))
        }
      } }
    } yield result
  }

  private def performRead(id: Option[UUID], auth: Option[APIKey]): Future[ValidationNel[ResourceError, List[Resource]]] = {
    import Permission._
    import PathData._

    Future { validateVersion(id) }.flatMap {
      case Some(version) => findResources(version, auth, accountId => Set(ReadPermission(path, WrittenByAccount(accountId))))
      case None =>
        val msg = "No matching version could be found for %s:%s".format(id.getOrElse("*"))
        logger.warn(msg)
        Promise.successful(Failure(MissingData(msg)).toValidationNel)
    }
  }

  def validateVersion(requested: Option[UUID]): Option[VersionEntry] = requested.flatMap {
    id => versionLog.find(id) orElse { throw new Exception("Could not locate version " + id) }
  } orElse versionLog.current

  def hasData(version: Option[UUID]): Boolean = {
    version.flatMap({ v => Option(mainVersionDir(v).list).map(_.length) }).exists(_ > 0)
  }

  def postStop = {
    Await.result(versions.values.flatMap(_.values).toStream.traverse(_.close), shutdownTimeout)
  }

  def receive = {
    case Create(_, data, id, authorities, overwrite) =>
      if (!overwrite && hasData(versionLog.current.map(_.id))) {
        sender ! UpdateFailure(path, GeneralError("Non-overwrite create request on existing data"))
      } else {
        Future {
          performCreate(data, id, authorities).map {
            _ map {
              _.fold(
                e => UpdateFailure(path, e),
                _ => UpdateSuccess(path)
              )
            } getOrElse UpdateSuccess(path)
          } unsafePerformIO
        } recover {
          case t: Throwable =>
            val msg = "Error during data creation " + path
            logger.error(msg, t)
            UpdateFailure(path, GeneralError(msg))
        } pipeTo sender
      }

    case Append(_, data, idOpt, authorities) =>
      (data match {
        case bd: BlobData =>
          Future {
            val errMsg = "Append not yet supported for blob data"
            logger.error(errMsg)
            UpdateFailure(path, GeneralError(errMsg))
          }

        case NIHDBData(events) =>
          // Which version are we appending to? In order, prefer the
          // provided version, then the current version. If no version
          // is available, we'll create a new one (legacy ingest
          // handling)
          Future { validateVersion(idOpt).map(_.id) }.flatMap {
            case Some(version) =>
              for {
                projection <- createProjection(version, authorities)
                result <- projection.db.insert(events)
              } yield UpdateSuccess(path)

            case None =>
              val version = UUID.randomUUID

              Future {
                (for {
                  _ <- createVersion(version, data.typeName)
                  _ <- promoteVersion(version)
                  created <- performCreate(data, version, Some(authorities))
                } yield {
                  created map { _.fold(
                    e => UpdateFailure(path, e),
                    _ => UpdateSuccess(path)
                  ) }
                }) unsafePerformIO
              }
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
