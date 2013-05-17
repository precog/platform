package com.precog.yggdrasil
package vfs

import Resource._
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
import scalaz.EitherT._
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
abstract class VFS[M[+_]](implicit M: Monad[M]) {
  protected def IOT: IO ~> M

  def readResource(path: Path, version: Version): EitherT[M, ResourceError, Resource]  

  def readQuery(path: Path, version: Version): EitherT[M, ResourceError, String] = {
    readResource(path, version) flatMap {
      case blob: BlobResource => right(IOT { blob.asString })
      case _ => left(NotFound("Requested resource at %s version %s cannot be interpreted as a Quirrel query.".format(path.path, version)).point[M])
    }
  }

  def readProjection(path: Path, version: Version): EitherT[M, ResourceError, StreamT[M, Slice]]

  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Slice]): StreamT[M, Slice] 
}

class ActorVFS(projectionsActor: ActorRef, clock: Clock, projectionReadTimeout: Timeout, sliceIngestTimeout: Timeout)(implicit M: Monad[Future]) extends VFS[Future] with Logging {

  val IOT = new (IO ~> Future) {
    def apply[A](io: IO[A]) = M.point(io.unsafePerformIO)
  }
  
  def readResource(path: Path, version: Version): EitherT[Future, ResourceError,Resource] = {
    implicit val t = projectionReadTimeout
    EitherT {
      (projectionsActor ? Read(path, version)).mapTo[ReadResult] map {
        case ReadSuccess(_, resource) => \/.right(resource)
        case ReadFailure(_, error) => \/.left(error)
      }
    }
  }

  def readProjection(path: Path, version: Version): EitherT[Future, ResourceError, StreamT[Future, Slice]] = {
    readResource(path, version) flatMap {
      case nihdb: NIHDBResource => right(nihdb.projection.map(_.getBlockStream(None)))
      case _ => left(NotFound("Requested resource at %s version %s cannot be interpreted as a Quirrel dataset.".format(path.path, version)).point[Future])
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
