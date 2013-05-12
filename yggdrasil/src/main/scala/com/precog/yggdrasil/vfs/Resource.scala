package com.precog
package yggdrasil
package vfs

import akka.actor.ActorSystem
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.core.http.{MimeType, MimeTypes}
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._

import com.precog.common.security.Authorities
import com.precog.niflheim.NIHDB
import com.precog.util.{IOUtils, PrecogUnit}

import java.io.{File, FileInputStream, FileOutputStream}

import org.joda.time.DateTime

import scala.annotation.tailrec

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect.IO
import scalaz.syntax.std.option._

import shapeless._

object Resource {
  val QuirrelData = MimeType("application", "x-quirrel-data")
}

sealed trait Resource {
  def mimeType: MimeType
  def authorities: Authorities
  def close: Future[PrecogUnit]
  def append(data: PathData): IO[PrecogUnit]
}

sealed trait ResourceError {
  def message: String
  def exception: Throwable
}

object ResourceError {
  case class CorruptData(message: String) extends ResourceError {
    val exception = new Exception(message)
  }

  case class MissingData(message: String) extends ResourceError {
    val exception = new Exception(message)
  }

  case class IOError(exception: Throwable) extends ResourceError {
    val message = Option(exception.getMessage) getOrElse exception.getClass.toString
  }

  case class GeneralError(message: String) extends ResourceError {
    val exception = new Exception(message)
  }

  case class PermissionsError(message: String) extends ResourceError {
    val exception = new Exception(message)
  }

  def fromExtractorError(msg: String): Extractor.Error => ResourceError = { error =>
    CorruptData("%s:\n%s" format (msg, error.message))
  }

  def fromExtractorErrorNel(msg: String): Extractor.Error => NEL[ResourceError] = { error =>
    NEL(fromExtractorError(msg)(error))
  }

  implicit val show = Show.showFromToString[ResourceError]
}

case class NIHDBResource(db: NIHDB, authorities: Authorities)(implicit as: ActorSystem) extends Resource {
  val mimeType: MimeType = Resource.QuirrelData
  def close: Future[PrecogUnit] = db.close(as)
  def append(data: PathData): IO[PrecogUnit] = data match {
    case NIHDBData(batch) =>
      db.insert(batch)

    case _ => 
      IO.throwIO(new IllegalArgumentException("Attempt to insert non-event data to NIHDB"))
  }
}

case class BlobMetadata(mimeType: MimeType, size: Long, created: DateTime, authorities: Authorities)

object BlobMetadata {
  implicit val iso = Iso.hlist(BlobMetadata.apply _, BlobMetadata.unapply _)

  implicit val mimeTypeDecomposer: Decomposer[MimeType] = new Decomposer[MimeType] {
    def decompose(u: MimeType) = JString(u.toString)
  }

  implicit val mimeTypeExtractor: Extractor[MimeType] = new Extractor[MimeType] {
    def validated(jv: JValue) = jv.validated[String].flatMap { ms =>
      MimeTypes.parseMimeTypes(ms).headOption.toSuccess(Error.invalid("Could not extract mime type from '%s'".format(ms)))
    }
  }

  import IsoSerialization._
  val schema = "mimeType" :: "size" :: "created" :: "authorities" :: HNil
  implicit val decomposer = decomposerV[BlobMetadata](schema, Some("1.0".v))
  implicit val extractor = extractorV[BlobMetadata](schema, Some("1.0".v))
}

/**
 * A blob of data that has been persisted to disk.
 */
final case class Blob(dataFile: File, metadata: BlobMetadata)(implicit ec: ExecutionContext) extends Resource {
  val authorities: Authorities = metadata.authorities
  val mimeType: MimeType = metadata.mimeType

  /** Suck the file into a String */
  def asString: IO[String] = IOUtils.readFileToString(dataFile)

  /** Stream the file off disk. */
  def stream: StreamT[IO, Array[Byte]] = {

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

  def close = Promise.successful(PrecogUnit)
}

object Blob {
  val ChunkSize = 100 * 1024
}
