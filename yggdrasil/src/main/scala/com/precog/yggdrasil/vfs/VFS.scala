package com.precog.yggdrasil
package vfs

import table.Slice
import metadata.PathMetadata
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
    def byteStream(requestedMimeTypes: Seq[MimeType])(implicit M: Monad[M]): OptionT[M, (MimeType, StreamT[M, Array[Byte]])]

    def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A): A

    protected def asByteStream(mimeType: MimeType)(implicit M: Monad[M]): OptionT[M, StreamT[M, Array[Byte]]]
  }

  object Resource {
    def asQuery(path: Path, version: Version)(implicit M: Monad[M]): Resource => EitherT[M, ResourceError, String] = { resource =>
      def notAQuery = notFound("Requested resource at %s version %s cannot be interpreted as a Quirrel query.".format(path.path, version))
      EitherT {
        resource.fold(
          br => br.asString.run.map(_.toRightDisjunction(notAQuery)),
          _ => \/.left(notAQuery).point[M]
        )
      }
    }

    def asProjection(path: Path, version: Version)(implicit M: Monad[M]): Resource => EitherT[M, ResourceError, Projection] = { resource =>
      def notAProjection = notFound("Requested resource at %s version %s cannot be interpreted as a Quirrel projection.".format(path.path, version))
      resource.fold(
        _ => EitherT.left(notAProjection.point[M]),
        pr => EitherT.right(pr.projection)
      )
    }
  }

  trait ProjectionResource extends Resource {
    def append(data: NIHDB.Batch): IO[PrecogUnit]
    def recordCount(implicit M: Monad[M]): M[Long]
    def projection(implicit M: Monad[M]): M[Projection]

    def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A) = projectionResource(this)

    def byteStream(requestedMimeTypes: Seq[MimeType])(implicit M: Monad[M]): OptionT[M, (MimeType, StreamT[M, Array[Byte]])] = {
      import FileContent._
      // Map to the type we'll use for conversion and the type we report to the user
      // FIXME: We're dealing with MimeType in too many places here
      val acceptableMimeTypes = ((Seq(ApplicationJson, XJsonStream, TextCSV).map { mt => mt -> (mt, mt) }) ++
        Seq(AnyMimeType -> (XJsonStream, XJsonStream), OctetStream -> (XJsonStream, OctetStream))).toMap
      for {
        selectedMT <- OptionT(M.point(requestedMimeTypes.find(acceptableMimeTypes.contains)))
        (conversionMT, returnMT) = acceptableMimeTypes(selectedMT)
        stream <- asByteStream(conversionMT)
      } yield (returnMT, stream)
    }
  }

  trait BlobResource extends Resource {
    def asString(implicit M: Monad[M]): OptionT[M, String]
    def byteLength: Long

    def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A) = blobResource(this)

    def byteStream(requestedMimeTypes: Seq[MimeType])(implicit M: Monad[M]): OptionT[M, (MimeType, StreamT[M, Array[Byte]])] = {
      import FileContent._
      val acceptableMimeTypes = Map(mimeType -> mimeType, AnyMimeType -> mimeType, OctetStream -> OctetStream)
      for {
        selectedMT <- OptionT(M.point(requestedMimeTypes.find(acceptableMimeTypes.contains)))
        stream     <- asByteStream(selectedMT)
      } yield (selectedMT, stream)
    }
  }

  trait VFSCompanionLike {
    def toJsonElements(block: Block): Vector[JValue]
    def derefValue(block: Block): Block
    def blockSize(block: Block): Int
    def pathStructure(selector: CPath)(implicit M: Monad[M]): Projection => EitherT[M, ResourceError, PathStructure]
  }

  type VFSCompanion <: VFSCompanionLike
  def VFS: VFSCompanion

  /**
   * VFS is an unsecured interface to the virtual filesystem; validation must be performed higher in the stack.
   */
  abstract class VFS(implicit M: Monad[M]) { 
    def writeAll(data: Seq[(Long, EventMessage)]): IO[PrecogUnit]

    def writeAllSync(data: Seq[(Long, EventMessage)]): EitherT[M, ResourceError, PrecogUnit]

    def readResource(path: Path, version: Version): EitherT[M, ResourceError, Resource]  

    /**
     * Returns the direct children of path.
     *
     * The results are the basenames of the children. So for example, if
     * we have /foo/bar/qux and /foo/baz/duh, and path=/foo, we will
     * return (bar, baz).
     */
    def findDirectChildren(path: Path): EitherT[M, ResourceError, Set[PathMetadata]]

    def findPathMetadata(path: Path): EitherT[M, ResourceError, PathMetadata]

    def currentVersion(path: Path): M[Option[VersionEntry]]
  }
}


