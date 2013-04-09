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

import com.precog.common.json._

import blueeyes.core.http._
import blueeyes.json._
import blueeyes.json.serialization._

import org.joda.time._

import shapeless._
import scalaz._




object Resource {
  val QuirrelData = MimeType("application", "x-quirrel-data")
}

sealed trait Resource {
  def mimeType: Future[MimeType]
  def authorities: Future[Authorities]
}





case class NIHDBResource(db: NIHDB)(implicit ec: ExecutionContext) {
  def mimeType: Future[MimeType] = Future(Resource.QuirrelData)
  def authorities: Future[Authorities] = db.authorities
}





// We may want to add a checksum field.
case class BlobMetadata(mimeType: MimeType, size: Long, created: DateTime, authorities: Authorities)

object BlobMetadata {
  implicit val iso = Iso.hlist(BlobMetadata.apply _, BlobMetadata.unapply _)

  import IsoSerialization._
  val schema = "mimeType" :: "size" :: "created" :: "authorities" :: HNil
  implicit val decomposer = decomposer[BlobMetadata](schema)
  implicit val extractor = extractor[BlobMetadata](schema)
}

/**
 * A blob of data that has been persisted to disk.
 */
final case class Blob(dataFile: File, metadata: BlobMetadata)(implicit ec: ExecutionContext) extends Resource {
  def authorities: Future[Authorities] = Future(metadata.authorities)
  def mimeType: Future[MimeType] = Future(metadata.mimeType)

  /** Stream the file off disk. */
  def stream: StreamT[IO, Array[Byte]] = {

    @tailrec
    def readChunk(fin: FileInputStream, skip: Int): Option[Array[Byte]] = {
      val remaining = skip - fin.skip(skip)
      if (remaining == 0) {
        val bytes = new Array[Byte](Blob.ChunkSize)
        val read = fin.read(bytes)

        if (read < 0) None
        else if (read == bytes.length) Some(bytes)
        else Some(java.util.Arrays.copyOf(bytes, read)

      } else {
        readChunk(remaining)
      }
    }

    StreamT.unfoldM[IO, Array[Byte], Long](0L) { offset =>
      IO(new FileInputStream(dataFile)).bracket(IO(_.close())) { in =>
        IO(readChunk(in, offset) map { bytes =>
          (bytes, offset + bytes.length)
        })
      }
    }
  }
}

object Blob {
  val ChunkSize = 100 * 1024
}




sealed trait ResourceError {
  def message: String
}

object ResourceError {
  case class CorruptData(message: String) extends ResourceError
  case class IOError(source: Exception) extends ResourceError

  def fromExtractorError(msg: String): Extractor.Error => ResourceError = { error =>
    CorruptData("%s:\n%s" format (msg, error.message))
  }
}


// trait Resource[A] {
//   def open(baseDir: File): IO[Validation[ResourceError, A]]
//   // def create(baseDir: File,
// 
//   def mimeType(a: A): MimeType
//   def authorities(a: A): Authorities
// }


/**
 * Used to access resources. This is needed because opening a NIHDB requires
 * more than just a basedir, but also things like the chef, txLogScheduler, etc.
 * This also goes for blobs, where the metadata log requires the txLogScheduler.
 */
trait Resources {
  def actorSystem: ActorSystem
  def clock: Clock
  def chef: ActorRef
  def cookThreshold: Int
  def storageTimeout: Timeout
  def permissionsFinder: PermissionsFinder[Future]
  def txLogSchedulerSize: Int = 20 // default for now, should come from config in the future

  import ResourceError._

  private final val txLogScheduler = new ScheduledThreadPoolExecutor(txLogSchedulerSize,
    new ThreadFactoryBuilder().setNameFormat("HOWL-sched-%03d").build())

  def createNIHDB(baseDir: File, authorities: Authorities): IO[Validation[ResourceError, NIHDBResource]] = {
    val dbIO = NIHDB.create(chef, authorities, baseDir,
      cookThreshold, storageTimeout, txLogScheduler)(actorSystem)
    dbIO map { dbV =>
      fromExtractorError("Failed to create NIHDB") <-: (dbV map (NIHDBResource(_)(actorSystem.dispatcher)))
    }
  }

  def openNIHDB(baseDir: File): IO[Validation[ResourceError, NIHDBResource]] = {
    val dbIO = NIHDB.open(chef, baseDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem)
    dbIO map (_ map { dbV =>
      val resV = dbV map (NIHDBResource(_)(actorSystem.dispatcher))
      fromExtractorError("Failed to open NIHDB") <-: resV :-> (_._2)
    }
  }

  /**
   * Open the blob for reading in `baseDir`.
   */
  def openBlob(baseDir: File): IO[Validtion[ResourceError, Blob]] = IO {
    val metadataStore = PersistentJValue(baseDir, "metadata")
    val metadata = metadataStore.validated[BlobMetadata]
    val resource = metadata map { metadata =>
      Blob(metadata, new File(baseDir, "data"))(actorSystem.dispatcher)
    }
    fromExtractorError("Error reading metadata") <-: resource
  }

  /**
   * Creates a blob from a data stream.
   */
  def createBlob[M[+_]: Monad](baseDir: File, mimeType: MimeType, authorities: Authorities,
      data: StreamT[M, Array[Byte]]): M[Validation[ResourceError, Blob]] = {

    val file = new File(baseDir, "data")
    val out = new FileOutputStream(file)
    def write(size: Long, stream: StreamT[M, Array[Byte]]): M[Validation[ResourceError, Long]] = {
      stream.uncons flatMap {
        case Some((bytes, tail)) =>
          try {
            out.write(bytes)
            write(size + bytes.length, tail)
          } catch { case (ioe: IOException) =>
            out.close()
            Failure(IOError(ioe)).point[M]
          }

        case None =>
          out.close()
          Success(size).point[M]
      }
    }

    write(0L, data) map (_ flatMap { size =>
      try {
        val metadata = BlobMetadata(mimeType, size, clock.now(), authorities)
        val metadataStore = PersistentJValue(baseDir, "metadata")
        metadataStore.json = metadata.serialize
        Success(Blob(metadata, file)(actorSystem.dispatcher))
      } catch { case (ioe: IOException) =>
        Failure(IOError(ioe))
      }
    })
  }
}


trait ResourceCreator[A, B <: Resource] {
  def create(baseDir: File, data: A): IO[Validation[ResourceError, B]]
}





sealed trait PathOp
case class Create[A, B <: Resource](data: A, auth: Option[APIKey], creator: ResourceCreator[A, B]) extends PathOp
case class Read(auth: Option[APIKey]) extends PathOp
case class Archive(auth: Option[APIKey]) extends PathOp
case class Execute(auth: Option[APIKey]) extends PathOp
case class Stat(auth: Option[APIKey]) extends PathOp


final case class VersionLog(versions: SortedMap[Long, String]) {
  def current: Long = versions.foldLeft(0L)(math.max)
  def +(kv: (Long, String)) = VersionLog(versions + kv)
}

object VersionLog {
  def empty = VersionLog(SortedMap.empty)

  implicit def extractor = new Extractor[VersionLog] {
    def validated(jvalue: JValue): Validation[Extractor.Error, VersionLog] = {
      jvalue match {
        case JArray(elems) =>
          val versionsV: Validation[Extractor.Error, List[(Long, String)]] = elems.traverse { jobj =>
            (jobj.validated[Long] |@| jobj.validated[String]).tupled
          }
          versionsV map { versions => VersionLog(SortedMap(versions: _*)) }

        case _ =>
          Failure(Extractor.invalid("Expected a top-level JSON array."))
      }
    }
  }

  implicit def decomposer = new Decomposer[VersionLog] {
    def decompose(log: VersionLog): JValue =
      JArray(log.versions.toList map { case (v, p) =>
        JObject(JField("version", JNum(v)) ::
                JField("path", JString(p)) ::
                Nil)
      })
  }
}

final case class PathManagerActor(baseDir: File, resources: Resources) extends Actor {
  private def versionSubdir(version: Long) = "v%020d" format version

  private val versionStore = PersistentJValue(baseDir, "versions")
  private var versions: VersionLog =
    versionStore.json.validated[VersionLog].valueOr(_ => VersionLog.empty)

  private def rollOver[A, B](f: File => Validation[A, B]): Validation[A, B] = {
    val v = versions.current
    val dir = versionSubdir(v)
    val file = new File(baseDir, dir)
    file.mkdirs()
    f(file) map { b =>
      versions += (v -> dir)
      versionStore.json = versions.serialize
      b
    }
  }

  def receive = {
    case Read(auth) =>
      // Returns a stream of bytes + mimetype
    case Archive(auth) =>
      rollOver(identity)
    case Create(data, auth, creator) =>
      rollOver(creator.create(_, data))
    case Execute(auth) =>
      // Return projection snapshot if possible.
    case Stat(_) =>
      // Owners, a bit of history, mime type, etc.
  }
}
