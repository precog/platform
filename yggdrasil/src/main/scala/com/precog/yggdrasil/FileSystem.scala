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

import com.google.common.cache.RemovalCause

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
  def close: Future[PrecogUnit]
}

case class NIHDBResource(db: NIHDB)(implicit as: ActorSystem) extends Resource {
  def mimeType: Future[MimeType] = Future(Resource.QuirrelData)
  def authorities: Future[Authorities] = db.authorities
  def close: Future[PrecogUnit] = db.close(as)
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
    }

    StreamT.unfoldM[IO, Array[Byte], Long](0L) { offset =>
      IO(new FileInputStream(dataFile)).bracket(IO(_.close())) { in =>
        IO(readChunk(in, offset) map { bytes =>
          (bytes, offset + bytes.length)
        })
      }
    }
  }

  def close = Promise.successful(PrecogUnit)
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

sealed trait PathOp
case class Create[A, B <: Resource](data: A, id: ResourceId, auth: Option[APIKey]) extends PathOp

/**
  * Appends to a specified resource or the current resource, depending on the id field.
  */
case class Append[A, B <: Resource](data: A, id: Option[ResourceId], auth: Option[APIKey]) extends PathOp
case class Replace(id: ResourceId, auth: Option[APIKey]) extends PathOp
case class Read(auth: Option[APIKey]) extends PathOp
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

/**
  * An actor that manages resources under a given path. The baseDir is the version
  * subdir for the path.
  */
final case class PathManagerActor(baseDir: File, path: Path, resources: Resources) extends Actor {
  private def versionSubdir(version: Long): File = new File(baseDir, "v%020d" format version)

  private val versionStore = PersistentJValue(baseDir, "versions")
  private var versions: VersionLog =
    versionStore.json.validated[VersionLog].valueOr(_ => VersionLog.empty)

  private def rollOver[A, B](f: File => Validation[A, B]): Validation[A, B] = {
    val v = versions.current
    val dir = versionSubdir(v)
    dir.mkdirs()
    f(dir) map { b =>
      versions += (v -> dir.getCanonicalPath)
      versionStore.json = versions.serialize
      b
    }
  }

  // Keeps track of the open projections for a given version/authority pair
  private val resources = Cache.simple[VersionId, Map[Authorities, Resource]](
    OnRemoval {
      case (_, resourceMap, cause) if cause != RemovalCause.REPLACED => resourceMap.foreach(_.close)
    },
    ExpireAfterAccess(Duration(30, TimeUnit.Minutes))
  )

  private def openProjections(version: VersionId): Option[IO[List[NIHDBResource]]] = {
    resources.get(version).map { existing =>
      logger.debug("Found cached projections for v" + version)
      IO(existing.values.toList.collect { case nr: NIHDBResource => nr })
    } orElse {
      logger.debug("Opening new projections for v" + version)
      resources.findDescriptorDirs(versionSubdir(version)).map { projDirs =>
        projDirs.toList.map { bd =>
          resources.openNIHDB(bd).map { dbv =>
            dbv.map { nihdbResource =>
              resources += (version.current -> (resources.getOrElse(version, Map()) + (nihdbResource.authorities -> nihdbResource)))
              logger.debug("Cache for v%d updated to %s".format(version, resources.get(version)))
              nihdbResource
            }
          }.valueOr { error => sys.error("An error occurred opening the NIHDB projection in %s: %s".format(bd, error.message)) }
        }.sequence.map(_.flatten)
      }
    }
  }

  def aggregateProjections(version: VersionId, apiKey: Option[APIKey]): Future[Option[NIHDBResource]] = {
    findProjections(version, apiKey).flatMap {
      case Some(inputs) =>
        inputs.toNel traverse { ps =>
          if (ps.size == 1) {
            logger.debug("Aggregate on v%d with key %s returns a single projection".format(version, apiKey))
            Promise.successful(ps.head)(context.dispatcher)
          } else {
            logger.debug("Aggregate on v%d with key %s returns an aggregate projection".format(version, apiKey))
            for {
              authorities <- ps.traverse(_.authorities)
              snaps <- ps.traverse(_.getSnapshot)
            } yield {
              new NIHDBResource(new NIHDBAggregate(authorities, snaps))
            }
          }
        }

      case None =>
        logger.debug("No projections found for " + path)
        Promise.successful(None)(context.dispatcher)
    }
  }

  def findProjections(version: VersionId, apiKey: Option[APIKey]): Future[Option[List[NIHDBProjection]]] = {
    import Permission._
    openProjections(version).map(_.unsafePerformIO) match {
      case None =>
        logger.warn("No projections found at " + path)
        Promise.successful(None)(context.dispatcher)

      case Some(foundProjections) =>
        logger.debug("Checking found projections on v" + version + " for access")
        foundProjections traverse { proj =>
          apiKey.map { key =>
            proj.authorities.flatMap { authorities =>
              // must have at a minimum a reduce permission from each authority
              // TODO: get a Clock in here
              authorities.accountIds.toList traverse { id =>
                apiKeyFinder.hasCapability(key, Set(ReducePermission(path, WrittenByAccount(id))), Some(new DateTime))
              } map {
                _.forall(_ == true).option(proj)
              }
            }
          } getOrElse {
            Promise.successful(Some(proj))(context.dispatcher)
          }
        } map {
          _.sequence
        }
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
