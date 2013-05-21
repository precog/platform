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
package vfs

import Resource._
import table.Slice
import metadata.PathStructure

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.niflheim.Reductions
import com.precog.yggdrasil.actor.IngestData
import com.precog.yggdrasil.nihdb.NIHDBProjection
import com.precog.util._

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

  def writeAll(data: Seq[(Long, EventMessage)]): IO[PrecogUnit]

  def readResource(path: Path, version: Version): EitherT[M, ResourceError, Resource]  

  def readQuery(path: Path, version: Version): EitherT[M, ResourceError, String] = {
    readResource(path, version) flatMap {
      case blob: BlobResource => right(IOT { blob.asString })
      case _ => left(NotFound("Requested resource at %s version %s cannot be interpreted as a Quirrel query.".format(path.path, version)).point[M])
    }
  }

  def readProjection(path: Path, version: Version): EitherT[M, ResourceError, ProjectionLike[M, Long, Slice]]

  /**
   * Returns the direct children of path.
   *
   * The results are the basenames of the children. So for example, if
   * we have /foo/bar/qux and /foo/baz/duh, and path=/foo, we will
   * return (bar, baz).
   */
  def findDirectChildren(path: Path): M[Set[Path]]

  def currentVersion(path: Path): M[Option[VersionEntry]]

  def currentSelectors(path: Path): EitherT[M, ResourceError, Set[CPath]] = {
    readProjection(path, Version.Current) flatMap { proj =>
      right(proj.structure.map(_.map(_.selector)))
    }
  }

  def currentStructure(path: Path, selector: CPath): EitherT[M, ResourceError, PathStructure]

  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Slice]): StreamT[M, Slice] 

}


class ActorVFS(projectionsActor: ActorRef, clock: Clock, projectionReadTimeout: Timeout, sliceIngestTimeout: Timeout)(implicit M: Monad[Future]) extends VFS[Future] with Logging {

  val IOT = new (IO ~> Future) {
    def apply[A](io: IO[A]) = M.point(io.unsafePerformIO)
  }

  def writeAll(data: Seq[(Long, EventMessage)]): IO[PrecogUnit] = {
    IO { projectionsActor ! IngestData(data) }
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

  // NIHDBProjection works in Future, so although this depends on 
  def readProjection(path: Path, version: Version): EitherT[Future, ResourceError, NIHDBProjection] = {
    readResource(path, version) flatMap {
      case nihdb: NIHDBResource => right(nihdb.projection)
      case _ => left(NotFound("Requested resource at %s version %s cannot be interpreted as a Quirrel dataset.".format(path.path, version)).point[Future])
    }
  }

  def findDirectChildren(path: Path): Future[Set[Path]] = {
    implicit val t = projectionReadTimeout
    val paths = (projectionsActor ? FindChildren(path)).mapTo[Set[Path]]
    paths map { _ flatMap { _ - path }}
  }

  def currentStructure(path: Path, selector: CPath): EitherT[Future, ResourceError, PathStructure] = {
    readProjection(path, Version.Current) flatMap { projection => 
      right {
        for (children <- projection.structure) yield {
          PathStructure(projection.reduce(Reductions.count, selector), children.map(_.selector))
        }
      }
    }
  }

  def currentVersion(path: Path) = {
    implicit val t = projectionReadTimeout
    (projectionsActor ? CurrentVersion(path)).mapTo[Option[VersionEntry]]
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
