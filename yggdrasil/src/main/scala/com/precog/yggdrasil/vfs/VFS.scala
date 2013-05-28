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
  }

  trait BlobResource extends Resource {
    def asString(implicit M: Monad[M]): OptionT[M, String]
    def byteLength: Long

    def fold[A](blobResource: BlobResource => A, projectionResource: ProjectionResource => A) = blobResource(this)
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
    def findDirectChildren(path: Path): EitherT[M, ResourceError, Set[Path]]

    def currentVersion(path: Path): M[Option[VersionEntry]]
  }
}


