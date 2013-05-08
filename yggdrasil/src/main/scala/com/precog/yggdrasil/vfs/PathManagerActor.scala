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
import akka.actor.ActorRef
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
import com.precog.common.ingest._
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
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._

case class IngestBundle(data: Seq[(Long, EventMessage)], perms: Map[APIKey, Set[Permission]])

/**
  * An actor that manages resources under a given path. The baseDir is the version
  * subdir for the path.
  */
final class PathManagerActor(path: Path, baseDir: File, versionLog: VersionLog, resources: DefaultResourceBuilder, permissionsFinder: PermissionsFinder[Future], shutdownTimeout: Duration, jobsManager: JobManager[Future], clock: Clock) extends Actor with Logging {
  import ResourceError._

  private[this] implicit def executor: ExecutionContext = context.dispatcher

  private[this] implicit val futureM = new FutureMonad(executor)

  // Keeps track of the resources for a given version/authority pair
  // TODO: make this an LRU cache
  private[this] var versions = Map[UUID, Resource]()

  override def postStop = {
    Await.result(versions.values.toStream.traverse(_.close), shutdownTimeout)
    versionLog.close
    logger.trace("Shutdown of path actor %s complete".format(path))
  }

  private def versionDir(version: UUID) = new File(baseDir, version.toString)

  private def checkReadPermissions[A](value: A, writtenBy: Authorities, apiKey: Option[APIKey], permsFor: AccountId => Set[Permission]): Future[Option[A]] = {
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

  private def openNIHDBProjection(version: UUID): Future[Option[ValidationNel[ResourceError, NIHDBProjection]]] = {
    Future { openNIHDB(version).unsafePerformIO } flatMap { _ traverse { _ traverse { NIHDBProjection.wrap _ } } }
  }

  private def canCreate(path: Path, permissions: Set[Permission], authorities: Authorities): Boolean = {
    PermissionsFinder.canWriteAs(permissions collect { case perm @ WritePermission(p, _) if p.isEqualOrParentOf(path) => perm }, authorities)
  }

  private def promoteVersion(version: UUID): IO[PrecogUnit] = {
    // we only promote if the requested version is in progress
    if (versionLog.isCompleted(version)) {
      IO(PrecogUnit)
    } else {
      versionLog.completeVersion(version)
    }
  }

  private def openResource(version: UUID): IO[Option[ValidationNel[ResourceError, Resource]]] = {
    versions.get(version) map { r =>
      IO(Some(Success(r)))
    } getOrElse {
      versionLog.find(version) traverse { versionEntry =>
        val dir = versionDir(version)
        val openf = if (NIHDB.hasProjection(dir)) { resources.openNIHDB _ }
                    else { resources.openBlob _ }

        openf(dir) flatMap {
          _ tap { resourceV =>
            IO(resourceV foreach { r => versions += (version -> r) })
          }
        }
      }
    }
  }

  private def openNIHDB(version: UUID): IO[Option[ValidationNel[ResourceError, NIHDBResource]]] = {
    versions.get(version) map {
      case nr: NIHDBResource => IO(Some(Success(nr)))
      case uhoh => IO(Some(Failure(nels(GeneralError("Located resource on %s is a BLOB, not a projection" format path)))))
    } getOrElse {
      versionLog.find(version) traverse { versionEntry =>
        resources.openNIHDB(versionDir(version)) flatMap {
          _ tap { resourceV =>
            IO(resourceV foreach { r => versions += (version -> r) })
          }
        }
      }
    }
  }

  private def performCreate(apiKey: APIKey, data: PathData, version: UUID, writeAs: Authorities, complete: Boolean): IO[PathActionResponse] = {
    for {
      _ <- versionLog.addVersion(VersionEntry(version, data.typeName))
      created <- data match {
        case BlobData(bytes, mimeType) =>
          resources.createBlob[IO](versionDir(version), mimeType, writeAs, bytes :: StreamT.empty[IO, Array[Byte]])

        case NIHDBData(data) =>
          resources.createNIHDB(versionDir(version), writeAs)
      }
      _ <- created traverse { resource =>
        for {
          _ <- IO { versions += (version -> resource) }
          _ <- complete.whenM(versionLog.completeVersion(version))
        } yield PrecogUnit
      }
    } yield {
      created.fold(
        error => UpdateFailure(path, nels(error)),
        _ => UpdateSuccess(path)
      )
    }
  }

  private def maybeCompleteJob(msg: EventMessage, terminal: Boolean, response: PathActionResponse) = {
    (response == UpdateSuccess(msg.path) && terminal).option(msg.jobId).join traverse { jobsManager.finish(_, clock.now()) } map { _ => response }
  }

  def processEventMessages(msgs: Stream[(Long, EventMessage)], permissions: Map[APIKey, Set[Permission]], requestor: ActorRef): IO[PrecogUnit] = {
    def persistNIHDB(createIfAbsent: Boolean, offset: Long, msg: IngestMessage, streamId: UUID, terminal: Boolean): IO[PrecogUnit] = {
      def batch(msg: IngestMessage) = NIHDB.Batch(offset, msg.data.map(_.value)) :: Nil

      openNIHDB(streamId) flatMap {
        case None =>
          if (createIfAbsent) {
            performCreate(msg.apiKey, NIHDBData(batch(msg)), streamId, msg.writeAs, terminal) map { response =>
              maybeCompleteJob(msg, terminal, response) pipeTo requestor
              PrecogUnit
            }
          } else {
            //TODO: update job
            IO(requestor ! UpdateFailure(path, nels(GeneralError("Cannot overwrite existing resource. %s not applied.".format(msg.toString)))))
          }

        case Some(Success(resource)) =>
          for {
            futureSuccess <- IO(resource.db.insert(batch(msg)))
            _ <- terminal.whenM(versionLog.completeVersion(streamId))
          } yield {
            futureSuccess flatMap { _ => maybeCompleteJob(msg, terminal, UpdateSuccess(msg.path)) } pipeTo requestor
            PrecogUnit
          }

        case Some(Failure(errors)) =>
          IO(requestor ! UpdateFailure(msg.path, errors))
      }
    }

    def persistFile(createIfAbsent: Boolean, offset: Long, msg: StoreFileMessage, streamId: UUID, terminal: Boolean): IO[PrecogUnit] = {
      if (createIfAbsent) {
        performCreate(msg.apiKey, BlobData(msg.content.data, msg.content.mimeType), streamId, msg.writeAs, terminal) map { response =>
          maybeCompleteJob(msg, terminal, response) pipeTo requestor
          PrecogUnit
        }
      } else {
        //TODO: update job
        IO(requestor ! UpdateFailure(path, nels(GeneralError("Cannot overwrite existing resource. %s not applied.".format(msg.toString)))))
      }
    }

    msgs traverse {
      case (offset, msg @ IngestMessage(apiKey, path, _, _, _, _, streamRef)) =>
        streamRef match {
          case StreamRef.Create(streamId, terminal) =>
            persistNIHDB(versionLog.current.isEmpty && !versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

          case StreamRef.Replace(streamId, terminal) =>
            persistNIHDB(!versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

          case StreamRef.Append =>
            val streamId = versionLog.current.map(_.id).getOrElse(UUID.randomUUID())
            persistNIHDB(canCreate(msg.path, permissions(apiKey), msg.writeAs), offset, msg, streamId, false)
        }

      case (offset, msg @ StoreFileMessage(_, path, _, _, _, _, _, streamRef)) =>
        streamRef match {
          case StreamRef.Create(streamId, terminal) =>
            persistFile(versionLog.current.isEmpty && !versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

          case StreamRef.Replace(streamId, terminal) =>
            persistFile(!versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

          case StreamRef.Append =>
            IO(requestor ! UpdateFailure(path, nels(GeneralError("Append is not yet supported for binary files."))))
        }

      case (offset, ArchiveMessage(apiKey, path, jobId, eventId, timestamp)) =>
        IO(sys.error("todo"): PrecogUnit)
    } map {
      _ => PrecogUnit
    }
  }


  def receive = {
    case IngestBundle(messages, permissions) =>
      processEventMessages(messages.toStream, permissions, sender).unsafePerformIO

    case Read(_, id, auth) =>
      val io: IO[Future[ReadResult]] = (id orElse versionLog.current.map(_.id)) map { version =>
        openResource(version) map {
          case Some(resourceV) =>
            resourceV traverse { resource =>
              checkReadPermissions(resource, resource.authorities, auth, id => Set(ReadPermission(path, WrittenByAccount(id)))) map { resOpt =>
                ReadSuccess(path, resOpt)
              }
            } map {
              _ valueOr { ReadFailure(path, _) }
            }

          case None =>
            Promise successful ReadFailure(path, nels(MissingData("Unable to find version %s".format(version))))
        }
      } getOrElse {
        IO(Promise successful ReadSuccess(path, None))
      } map { resVF =>
        resVF recover {
          case t: Throwable =>
            val msg = "Error during read on " + id
            logger.error(msg, t)
            ReadFailure(path, nels(IOError(t)))
        } pipeTo sender
      }

      io.unsafePerformIO

    case ReadProjection(_, id, auth) =>
      val projectionVF = (id orElse versionLog.current.map(_.id)) map { version =>
        openNIHDBProjection(version) map {
          case Some(resourceV) =>
            resourceV traverse { projection =>
              checkReadPermissions(projection, projection.authorities, auth, id => Set(ReducePermission(path, WrittenByAccount(id)))) map { projOpt =>
                ReadProjectionSuccess(path, projOpt)
              }
            } map {
              _ valueOr { ReadProjectionFailure(path, _) }
            }

          case None =>
            Promise successful ReadProjectionFailure(path, nels(MissingData("Unable to find version %s".format(version))))
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
