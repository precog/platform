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

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.core.http.{MimeType, MimeTypes}
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._

import com.precog.common.security.Authorities
import com.precog.common.ingest.FileContent
import com.precog.niflheim.NIHDB
import com.precog.yggdrasil.nihdb.NIHDBProjection
import com.precog.yggdrasil.table.Slice
import com.precog.util.{IOUtils, PrecogUnit}

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.CharBuffer
import java.nio.ByteBuffer
import java.nio.charset.{Charset, CoderResult}
import java.util.Arrays

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime


import scala.annotation.tailrec

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect.IO
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

object Resource extends Logging {
  val QuirrelData = MimeType("application", "x-quirrel-data")

  def toCharBuffers[N[+_]: Monad](output: MimeType, slices: StreamT[N, Slice]): StreamT[N, CharBuffer] = {
    import com.precog.yggdrasil.table.ColumnarTableModule
    import FileContent._
    output match {
      case ApplicationJson =>
        ColumnarTableModule.renderJson(slices, "[", ",", "]")

      case XJsonStream =>
        ColumnarTableModule.renderJson(slices, "", "\n", "")

      case TextCSV => 
        ColumnarTableModule.renderCsv(slices)

      case other => 
        logger.warn("Unrecognized output type requested for conversion of slice stream to char buffers: %s".format(output))
        StreamT.empty[N, CharBuffer]
    }
  }

  def bufferOutput(stream0: StreamT[Future, CharBuffer], charset: Charset = Charset.forName("UTF-8"), bufferSize: Int = 64 * 1024)(implicit M: Monad[Future]): StreamT[Future, Array[Byte]] = {
    val encoder = charset.newEncoder()

    def loop(stream: StreamT[Future, CharBuffer], buf: ByteBuffer, arr: Array[Byte]): StreamT[Future, Array[Byte]] = {
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

sealed trait Resource {
  def mimeType: MimeType
  def authorities: Authorities
  def append(data: PathData): IO[PrecogUnit]
  def byteStream(mimeType: Option[MimeType])(implicit M: Monad[Future]): Future[Option[StreamT[Future, Array[Byte]]]]
}

sealed trait ResourceError

object ResourceError {
  case class CorruptData(message: String) extends ResourceError 
  case class MissingData(message: String) extends ResourceError
  case class IOError(exception: Throwable) extends ResourceError

  case class IllegalWriteRequestError(message: String) extends ResourceError
  case class PermissionsError(message: String) extends ResourceError

  def fromExtractorError(msg: String): Extractor.Error => ResourceError = { error =>
    CorruptData("%s:\n%s" format (msg, error.message))
  }

  def fromExtractorErrorNel(msg: String): Extractor.Error => NEL[ResourceError] = { error =>
    NEL(fromExtractorError(msg)(error))
  }

  implicit val show = Show.showFromToString[ResourceError]
}

case class NIHDBResource(db: NIHDB, authorities: Authorities)(implicit as: ActorSystem) extends Resource with Logging {
  val mimeType: MimeType = Resource.QuirrelData
  def append(data: PathData): IO[PrecogUnit] = data match {
    case NIHDBData(batch) =>
      db.insert(batch)

    case BlobData(_, mimeType) => 
      IO.throwIO(new IllegalArgumentException("Attempt to insert non-event blob data of type %s to NIHDB".format(mimeType.value)))
  }

  def byteStream(mimeType: Option[MimeType])(implicit M: Monad[Future]): Future[Option[StreamT[Future, Array[Byte]]]] = {
    import Resource.{ bufferOutput, toCharBuffers }
    import FileContent._
    NIHDBProjection.wrap(this) map { p =>
      val sliceStream = p.getBlockStream(None)
      mimeType match {
        case Some(ApplicationJson) | None => Some(bufferOutput(toCharBuffers(ApplicationJson, sliceStream)))
        case Some(XJsonStream) => Some(bufferOutput(toCharBuffers(XJsonStream, sliceStream)))
        case Some(TextCSV) => Some(bufferOutput(toCharBuffers(TextCSV, sliceStream)))
        case Some(other) => 
          logger.warn("NIHDB resource cannot be rendered to a byte stream of type %s".format(other.value))
          None
      }
    }
  }
}

case class BlobMetadata(mimeType: MimeType, size: Long, created: DateTime, authorities: Authorities)

object BlobMetadata {
  import shapeless.{ Iso, HNil }
  import blueeyes.json.serialization.IsoSerialization._
  implicit val iso = Iso.hlist(BlobMetadata.apply _, BlobMetadata.unapply _)

  implicit val mimeTypeDecomposer: Decomposer[MimeType] = new Decomposer[MimeType] {
    def decompose(u: MimeType) = JString(u.toString)
  }

  implicit val mimeTypeExtractor: Extractor[MimeType] = new Extractor[MimeType] {
    def validated(jv: JValue) = jv.validated[String].flatMap { ms =>
      MimeTypes.parseMimeTypes(ms).headOption.toSuccess(Error.invalid("Could not extract mime type from '%s'".format(ms)))
    }
  }

  val schema = "mimeType" :: "size" :: "created" :: "authorities" :: HNil
  implicit val decomposer = decomposerV[BlobMetadata](schema, Some("1.0".v))
  implicit val extractor = extractorV[BlobMetadata](schema, Some("1.0".v))
}

/**
 * A blob of data that has been persisted to disk.
 */
final case class Blob(dataFile: File, metadata: BlobMetadata) extends Resource {
  val authorities: Authorities = metadata.authorities
  val mimeType: MimeType = metadata.mimeType

  /** Suck the file into a String */
  def asString: IO[String] = IOUtils.readFileToString(dataFile)

  /** Stream the file off disk. */
  def ioStream: StreamT[IO, Array[Byte]] = {
    @tailrec
    def readChunk(fin: FileInputStream, skip: Long): Option[Array[Byte]] = {
      val remaining = skip - fin.skip(skip)
      if (remaining == 0) {
        val bytes = new Array[Byte](Blob.ChunkSize)
        val read = fin.read(bytes)

        if (read < 0) None
        else if (read == bytes.length) Some(bytes)
        else Some(java.util.Arrays.copyOf(bytes, read))
      } else {
        readChunk(fin, remaining)
      }
    }

    StreamT.unfoldM[IO, Array[Byte], Long](0L) { offset =>
      IO(new FileInputStream(dataFile)).bracket(f => IO(f.close())) { in =>
        IO(readChunk(in, offset) map { bytes =>
          (bytes, offset + bytes.length)
        })
      }
    }
  }

  def byteStream(mimeType: Option[MimeType])(implicit M: Monad[Future]): Future[Option[StreamT[Future, Array[Byte]]]] = {
    val IOF: IO ~> Future = new NaturalTransformation[IO, Future] {
      def apply[A](io: IO[A]) = M.point(io.unsafePerformIO)
    }

    M point { (mimeType.forall(_ == metadata.mimeType)).option(ioStream.trans(IOF)) }
  }

  def append(data: PathData): IO[PrecogUnit] = data match {
    case _ => IO.throwIO(new IllegalArgumentException("Blob append not yet supported"))

//    case BlobData(bytes, mimeType) => Future {
//      if (mimeType != metadata.mimeType) {
//        throw new IllegalArgumentException("Attempt to append %s data to a %s blob".format(mimeType, metadata.mimeType))
//      }
//
//      IO(new FileOutputStream(dataFile, true)).bracket(f => IO(f.close())) { output =>
//        IO(output.write(bytes))
//      }.unsafePerformIO
//
//      PrecogUnit
//    }
//
//    case _ => Promise.failed(new IllegalArgumentException("Attempt to insert non-blob data to blob"))
  }
}

object Blob {
  val ChunkSize = 100 * 1024
}
