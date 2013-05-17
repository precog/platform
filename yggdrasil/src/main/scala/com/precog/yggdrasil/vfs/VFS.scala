package com.precog.yggdrasil
package vfs

import table.Slice

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.yggdrasil.actor.IngestData

import akka.dispatch.Future
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.util.Clock

import java.util.UUID
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.effect.IO

sealed trait Version
object Version {
  case object Current extends Version
  case class Archived(uuid: UUID) extends Version
}

/**
 * VFS is an unsecured interface to the virtual filesystem; validation must be performed higher in the stack.
 */
trait VFS[M[+_]] {
  def readQuery(path: Path, version: Version): M[Option[String]]
  def readResource(path: Path, version: Version): M[ReadResult]
  def readCache(path: Path, version: Version): M[Option[StreamT[M, Slice]]]
  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Slice]): StreamT[M, Slice]
}

object NoopVFS extends VFS[Future] {
  def readQuery(path: Path, version: Version): Future[Option[String]] = sys.error("stub VFS")
  def readResource(path: Path, version: Version): Future[ReadResult] = sys.error("stub VFS")
  def readCache(path: Path, version: Version): Future[Option[StreamT[Future, Slice]]] = sys.error("stub VFS")
  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[Future, Slice]): StreamT[Future, Slice]  = sys.error("stub VFS")
}

class ActorVFS(projectionsActor: ActorRef, clock: Clock, projectionReadTimeout: Timeout, sliceIngestTimeout: Timeout)(implicit M: Monad[Future]) extends VFS[Future] with Logging {
  def readQuery(path: Path, version: Version): Future[Option[String]] = {
    implicit val t = projectionReadTimeout
    (projectionsActor ? Read(path, version, None)).mapTo[ReadResult] map {
      case ReadSuccess(_, Some(blob: Blob)) =>
        blob.asString map(Some(_)) except { case t: Throwable => IO { logger.error("Query read error", t); None } } unsafePerformIO

      case failure =>
        logger.warn("Unable to read query at path %s with version %s: %s".format(path.path, version, failure))
        None
    }
  }

  def readResource(path: Path, version: Version): Future[ReadResult] = {
    implicit val t = projectionReadTimeout
    (projectionsActor ? Read(path, version, None)).mapTo[ReadResult]
  }

  def readCache(path: Path, version: Version): Future[Option[StreamT[Future, Slice]]] = {
    implicit val t = projectionReadTimeout
    (projectionsActor ? ReadProjection(path, version, None)).mapTo[ReadProjectionResult] map {
      case ReadProjectionSuccess(_, Some(projection)) => Some(projection.getBlockStream(None))
      case failure =>
        logger.warn("Unable to read projection at path %s with version %s: %s".format(path.path, version, failure))
        None
    }
  }

  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[Future, Slice]): StreamT[Future, Slice] = {
    implicit val askTimeout = sliceIngestTimeout
    val streamId = java.util.UUID.randomUUID()
    val allPerms = Map(apiKey -> perms)

    StreamT.unfoldM((0, stream)) {
      case (pseudoOffset, s) =>
        s.uncons flatMap {
          case Some((x, xs)) =>
            val ingestRecords = x.toJsonElements.zipWithIndex map {
              case (v, i) => IngestRecord(EventId(pseudoOffset, i), v)
            }

            logger.debug("Persisting %d stream records to %s".format(ingestRecords.size, path))

            for {
              terminal <- xs.isEmpty
              par <- {
                // FIXME: is Replace always desired here? Any case
                // where we might want Create? AFAICT, this is only
                // used for caching queries right now.
                val streamRef = StreamRef.Replace(streamId, terminal)
                val msg = IngestMessage(apiKey, path, writeAs, ingestRecords, jobId, clock.instant(), streamRef)
                (projectionsActor ? IngestData(Seq((pseudoOffset, msg)))).mapTo[PathActionResponse]
              }
            } yield {
              par match {
                case UpdateSuccess(_) =>
                  Some((x, (pseudoOffset + 1, xs)))
                case PathFailure(_, errors) =>
                  logger.error("Unable to complete persistence of result stream by %s to %s as %s: %s".format(apiKey, path.path, writeAs, errors.shows))
                  None
                case UpdateFailure(_, errors) =>
                  logger.error("Unable to complete persistence of result stream by %s to %s as %s: %s".format(apiKey, path.path, writeAs, errors.shows))
                  None

                case invalid =>
                  logger.error("Unexpected response to persist: " + invalid)
                  None
              }
            }

          case None =>
            None.point[Future]
        }
    }
  }
}
