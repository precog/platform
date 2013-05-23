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
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.syntax.std.list._
import scalaz.effect.IO

sealed trait Version
object Version {
  case object Current extends Version
  case class Archived(uuid: UUID) extends Version
}

/**
 * VFS is an unsecured interface to the virtual filesystem; validation must be performed higher in the stack.
 */
abstract class VFS[M[+_]](implicit M: Monad[M], IOT: IO ~> M) extends Logging { //FIXME: IOT is a worthless hack.
  def writeAll(data: Seq[(Long, EventMessage)]): IO[PrecogUnit]

  def writeAllSync(data: Seq[(Long, EventMessage)]): EitherT[M, ResourceError, PrecogUnit]

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

  //def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Slice]): StreamT[M, Slice] 

  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Slice], clock: Clock): StreamT[M, Slice] = {
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
                writeAllSync(Seq((pseudoOffset, msg))).run
              }
            } yield {
              par.fold(
                errors => {
                  logger.error("Unable to complete persistence of result stream by %s to %s as %s: %s".format(apiKey, path.path, writeAs, errors.shows))
                  None
                },
                _ => Some((x, (pseudoOffset + 1, xs)))
              )
            }

          case None =>
            None.point[M]
        }
    }
  }
}


