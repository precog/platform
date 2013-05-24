package com.precog.yggdrasil
package vfs

import table.Slice
import metadata.PathStructure

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.niflheim._
import com.precog.yggdrasil.actor.IngestData
import com.precog.yggdrasil.nihdb.NIHDBProjection
import com.precog.util._

import blueeyes.core.http.MimeType
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Decomposer._
import blueeyes.util.Clock

import java.util.UUID
import java.nio.ByteBuffer
import java.util.Arrays
import java.nio.CharBuffer
import java.nio.charset.{Charset, CoderResult}

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

object VFSModule {
  def bufferOutput[M[+_]: Monad](stream0: StreamT[M, CharBuffer], charset: Charset = Charset.forName("UTF-8"), bufferSize: Int = 64 * 1024): StreamT[M, Array[Byte]] = {
    val encoder = charset.newEncoder()

    def loop(stream: StreamT[M, CharBuffer], buf: ByteBuffer, arr: Array[Byte]): StreamT[M, Array[Byte]] = {
      StreamT(stream.uncons map {
        case Some((cbuf, tail)) =>
          val result = encoder.encode(cbuf, buf, false)
          if (result == CoderResult.OVERFLOW) {
            val arr2 = new Array[Byte](bufferSize)
            StreamT.Yield(arr, loop(cbuf :: tail, ByteBuffer.wrap(arr2), arr2))
          } else {
            StreamT.Skip(loop(tail, buf, arr))
          }

        case None =>
          val result = encoder.encode(CharBuffer.wrap(""), buf, true)
          if (result == CoderResult.OVERFLOW) {
            val arr2 = new Array[Byte](bufferSize)
            StreamT.Yield(arr, loop(stream, ByteBuffer.wrap(arr2), arr2))
          } else {
            StreamT.Yield(Arrays.copyOf(arr, buf.position), StreamT.empty)
          }
      })
    }

    val arr = new Array[Byte](bufferSize)
    loop(stream0, ByteBuffer.wrap(arr), arr)
  }
}

trait VFSModule[M[+_], Block] extends Logging {
  import ResourceError._

  type Projection <: ProjectionLike[M, Block]

  sealed trait Resource {
    def mimeType: MimeType
    def authorities: Authorities
    def byteStream(mimeType: Option[MimeType])(implicit M: Monad[M]): OptionT[M, StreamT[M, Array[Byte]]]

    def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A): A
  }

  trait ProjectionResource extends Resource {
    def append(data: NIHDB.Batch): IO[PrecogUnit]
    def recordCount(implicit M: Monad[M]): M[Long]
    def projection(implicit M: Monad[M]): M[Projection]

    def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A) = projectionResource(this)
  }

  trait BlobResource extends Resource {
    def asString(implicit M: Monad[M]): OptionT[M, String]
    def byteLength: Long

    def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A) = blobResource(this)
  }

  /**
   * VFS is an unsecured interface to the virtual filesystem; validation must be performed higher in the stack.
   */
  abstract class VFS(implicit M: Monad[M]) { 
    protected[this] def toJsonElements(block: Block): Vector[JValue]

    def writeAll(data: Seq[(Long, EventMessage)]): IO[PrecogUnit]

    def writeAllSync(data: Seq[(Long, EventMessage)]): EitherT[M, ResourceError, PrecogUnit]

    def readResource(path: Path, version: Version): EitherT[M, ResourceError, Resource]  

    def readQuery(path: Path, version: Version): EitherT[M, ResourceError, String] = {
      def notAQuery = notFound("Requested resource at %s version %s cannot be interpreted as a Quirrel query.".format(path.path, version))

      readResource(path, version) flatMap {
        _.fold(
          br => EitherT(br.asString.run.map(_.toRightDisjunction(notAQuery))),
          _ => EitherT.left(notAQuery.point[M])
        )
      }
    }

    def readProjection(path: Path, version: Version): EitherT[M, ResourceError, Projection] = {
      def notAProjection = notFound("Requested resource at %s version %s cannot be interpreted as a Quirrel projection.".format(path.path, version))
      readResource(path, version) flatMap {
        _.fold(
          _ => EitherT.left(notAProjection.point[M]),
          pr => EitherT.right(pr.projection)
        )
      }
    }

    def pathStructure(path: Path, selector: CPath, version: Version): EitherT[M, ResourceError, PathStructure]

    /**
     * Returns the direct children of path.
     *
     * The results are the basenames of the children. So for example, if
     * we have /foo/bar/qux and /foo/baz/duh, and path=/foo, we will
     * return (bar, baz).
     */
    def findDirectChildren(path: Path): M[Set[Path]]

    def currentVersion(path: Path): M[Option[VersionEntry]]

/*
    // TODO: currentStructure has all this information, need to deduplicate
    def currentSelectors(path: Path): EitherT[M, ResourceError, Set[CPath]] = {
      readProjection(path, Version.Current) flatMap { proj =>
        right(proj.structure.map(_.map(_.selector)))
      }
    }
    */

    def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Block], clock: Clock): StreamT[M, Block] = {
      val streamId = java.util.UUID.randomUUID()
      val allPerms = Map(apiKey -> perms)

      StreamT.unfoldM((0, stream)) {
        case (pseudoOffset, s) =>
          s.uncons flatMap {
            case Some((x, xs)) =>
              val ingestRecords = toJsonElements(x).zipWithIndex map {
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
}


