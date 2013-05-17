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
import scalaz.EitherT._
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
  import Resource._

  private[this] implicit def executor: ExecutionContext = context.dispatcher

  private[this] implicit val futureM = new FutureMonad(executor)

  // Keeps track of the resources for a given version/authority pair
  // TODO: make this an LRU cache
  private[this] var versions = Map[UUID, Resource]()

  override def postStop = {
    val closeAll = versions.values.toStream traverse {
      case NIHDBResource(db, _) => db.close(context.system)
      case _ => Promise successful PrecogUnit
    }
    
    Await.result(closeAll, shutdownTimeout)
    versionLog.close
    logger.info("Shutdown of path actor %s complete".format(path))
  }

  private def versionDir(version: UUID) = new File(baseDir, version.toString)

  //FIXME: this is the wrong level to be doing these permissions checks. This needs to have been done way higher up in the stack.
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

  private def canCreate(path: Path, permissions: Set[Permission], authorities: Authorities): Boolean = {
    logger.trace("Checking write permission for " + path + " as " + authorities + " among " + permissions)
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

  private def openResource(version: UUID): EitherT[IO, ResourceError, Resource] = {
    versions.get(version) map { r =>
      logger.debug("Located existing resource for " + version)
      right(IO(r))
    } getOrElse {
      logger.debug("Opening new resource for " + version)
      versionLog.find(version) map {
        case VersionEntry(v, _, _) =>
          val dir = versionDir(v)
          val openf = if (NIHDB.hasProjection(dir)) { resources.openNIHDB _ }
                      else { resources.openBlob _ }

          for {
            resource <- EitherT {
              openf(dir) flatMap {
                _ tap { resourceV =>
                  IO(resourceV foreach { r => versions += (version -> r) })
                }
              }
            }
          } yield resource 
      } getOrElse {
        left(IO(NotFound("No version found to exist for resource %s.".format(path.path))))
      }
    }
  }

  private def openNIHDB(version: UUID): EitherT[IO, ResourceError, NIHDBResource] = {
    openResource(version) flatMap {
      case nr: NIHDBResource => right(IO(nr))
      case other => left(IO(NotFound("Located resource on %s is a BLOB, not a projection" format path.path)))
    }
  }

  private def performCreate(apiKey: APIKey, data: PathData, version: UUID, writeAs: Authorities, complete: Boolean): IO[PathActionResponse] = {
    implicit val ioId = NaturalTransformation.refl[IO]
    for {
      _ <- versionLog.addVersion(VersionEntry(version, data.typeName, clock.instant()))
      created <- data match {
        case BlobData(bytes, mimeType) =>
          resources.createBlob[IO](versionDir(version), mimeType, writeAs, bytes :: StreamT.empty[IO, Array[Byte]])

        case NIHDBData(data) =>
          resources.createNIHDB(versionDir(version), writeAs) flatMap {
            _ traverse { _ tap { _.db.insert(data) } }
          }
      }
      _ <- created traverse { resource =>
        for {
          _ <- IO { versions += (version -> resource) }
          _ <- complete.whenM(versionLog.completeVersion(version))
        } yield PrecogUnit
      }
    } yield {
      created.fold(
        error => UpdateFailure(path, error),
        _ => UpdateSuccess(path)
      )
    }
  }

  private def maybeCompleteJob(msg: EventMessage, terminal: Boolean, response: PathActionResponse) = {
    //TODO: Add job progress updates
    (response == UpdateSuccess(msg.path) && terminal).option(msg.jobId).join traverse { jobsManager.finish(_, clock.now()) } map { _ => response }
  }

  def processEventMessages(msgs: Stream[(Long, EventMessage)], permissions: Map[APIKey, Set[Permission]], requestor: ActorRef): IO[PrecogUnit] = {
    logger.debug("About to persist %d messages; replying to %s".format(msgs.size, requestor.toString))

    def persistNIHDB(createIfAbsent: Boolean, offset: Long, msg: IngestMessage, streamId: UUID, terminal: Boolean): IO[PrecogUnit] = {
      def batch(msg: IngestMessage) = NIHDB.Batch(offset, msg.data.map(_.value)) :: Nil

      openNIHDB(streamId).fold[IO[PrecogUnit]](
        {
          case NotFound(_) =>
            if (createIfAbsent) {
              logger.trace("Creating new nihdb database for streamId " + streamId)
              performCreate(msg.apiKey, NIHDBData(batch(msg)), streamId, msg.writeAs, terminal) map { response =>
                maybeCompleteJob(msg, terminal, response) pipeTo requestor
                PrecogUnit
              }
            } else {
              //TODO: update job
              logger.warn("Cannot overwrite existing database for " + streamId)
              IO(requestor ! UpdateFailure(path, IllegalWriteRequestError("Cannot overwrite existing resource. %s not applied.".format(msg.toString))))
            }

          case other =>
            IO(requestor ! UpdateFailure(path, other))
        },
        resource => for {
          _ <- resource.db.insert(batch(msg))
          _ <- terminal.whenM(versionLog.completeVersion(streamId) >> versionLog.setHead(streamId))
        } yield {
          logger.trace("Sent insert message for " + msg + " to nihdb")
          // FIXME: We aren't actually guaranteed success here because NIHDB might do something screwy.
          maybeCompleteJob(msg, terminal, UpdateSuccess(msg.path)) pipeTo requestor
          PrecogUnit
        }
      ).join
    }

    def persistFile(createIfAbsent: Boolean, offset: Long, msg: StoreFileMessage, streamId: UUID, terminal: Boolean): IO[PrecogUnit] = {
      logger.debug("Persisting file on %s for offset %d".format(path, offset))
      // TODO: I think the semantics here of createIfAbsent aren't
      // quite right. If we're in a replay we don't want to return
      // errors if we're already complete
      if (createIfAbsent) {
        for {
          response <- performCreate(msg.apiKey, BlobData(msg.content.data, msg.content.mimeType), streamId, msg.writeAs, terminal)
          _ <- terminal.whenM(versionLog.setHead(streamId))
        } yield {
          maybeCompleteJob(msg, terminal, response) pipeTo requestor
          PrecogUnit
        }
      } else {
        //TODO: update job
        IO(requestor ! UpdateFailure(path, IllegalWriteRequestError("Cannot overwrite existing resource. %s not applied.".format(msg.toString))))
      }
    }

    msgs traverse {
      case (offset, msg @ IngestMessage(apiKey, path, _, _, _, _, streamRef)) =>
        streamRef match {
          case StreamRef.Create(streamId, terminal) =>
            logger.trace("Received create for %s stream %s: current: %b, complete: %b".format(path.path, streamId, versionLog.current.isEmpty, versionLog.isCompleted(streamId)))
            persistNIHDB(versionLog.current.isEmpty && !versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

          case StreamRef.Replace(streamId, terminal) =>
            logger.trace("Received replace for %s stream %s: complete: %b".format(path.path, streamId, versionLog.isCompleted(streamId)))
            persistNIHDB(!versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

          case StreamRef.Append =>
            logger.trace("Received append for %s".format(path.path))
            val streamId = versionLog.current.map(_.id).getOrElse(UUID.randomUUID())
            for {
              _ <- persistNIHDB(canCreate(msg.path, permissions(apiKey), msg.writeAs), offset, msg, streamId, false)
              _ <- versionLog.completeVersion(streamId)
              _ <- versionLog.setHead(streamId)
            } yield PrecogUnit
        }

      case (offset, msg @ StoreFileMessage(_, path, _, _, _, _, _, streamRef)) =>
        streamRef match {
          case StreamRef.Create(streamId, terminal) =>
            if (! terminal) {
              logger.warn("Non-terminal BLOB for %s will not currently behave correctly!".format(path))
            }
            persistFile(versionLog.current.isEmpty && !versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

          case StreamRef.Replace(streamId, terminal) =>
            if (! terminal) {
              logger.warn("Non-terminal BLOB for %s will not currently behave correctly!".format(path))
            }
            persistFile(!versionLog.isCompleted(streamId), offset, msg, streamId, terminal)

          case StreamRef.Append =>
            IO(requestor ! UpdateFailure(path, IllegalWriteRequestError("Append is not yet supported for binary files.")))
        }

      case (offset, ArchiveMessage(apiKey, path, jobId, eventId, timestamp)) =>
        versionLog.clearHead >> IO(requestor ! UpdateSuccess(path))
    } map {
      _ => PrecogUnit
    }
  }

  def versionOpt(version: Version) = version match {
    case Version.Archived(id) => Some(id)
    case Version.Current => versionLog.current.map(_.id)
  }

  def receive = {
    case IngestBundle(messages, permissions) =>
      logger.debug("Received ingest request for %d messages.".format(messages.size))
      processEventMessages(messages.toStream, permissions, sender).unsafePerformIO

    case msg @ Read(_, version) =>
      logger.debug("Received Read request " + msg)

      val requestor = sender
      val io: IO[ReadResult] = versionOpt(version) map { version =>
        openResource(version).fold(
          error => ReadFailure(path, error),
          resource => ReadSuccess(path, resource) 
        )
      } getOrElse {
        IO(ReadFailure(path, Corrupt("Unable to determine any resource version for path %s".format(path.path))))
      } 
      
      io.map(requestor ! _).unsafePerformIO

    case CurrentVersion(_) =>
      sender ! versionLog.current
  }
}
