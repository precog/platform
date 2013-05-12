package com.precog.yggdrasil
package vfs

import table.Slice

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._

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
  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Slice]): StreamT[M, Slice] 
}

class ActorVFS(projectionsActor: ActorRef, clock: Clock, queryReadTimeout: Timeout, sliceIngestTimeout: Timeout)(implicit M: Monad[Future]) extends VFS[Future] with Logging {
  def readQuery(path: Path, version: Version): Future[Option[String]] = {
    implicit val t = queryReadTimeout
    (projectionsActor ? Read(path, None, None)).mapTo[ReadResult] map {
      case ReadSuccess(_, Some(blob: Blob)) =>
        blob.asString map(Some(_)) except { case t: Throwable => IO { logger.error("Query read error", t); None } } unsafePerformIO

      case _ => None
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

            for {
              terminal <- xs.isEmpty
              par <- {
                val streamRef = StreamRef.Create(streamId, terminal)
                val msg = IngestMessage(apiKey, path, writeAs, ingestRecords, jobId, clock.instant(), streamRef)
                (projectionsActor ? IngestBundle(Seq((pseudoOffset, msg)), allPerms)).mapTo[PathActionResponse]
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
              }
            }

          case None => 
            None.point[Future]
        }
    }
  }
}

