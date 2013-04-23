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

import java.util.UUID

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
  def append(data: PathData): Future[PrecogUnit]
}

case class NIHDBResource(db: NIHDB)(implicit as: ActorSystem) extends Resource {
  def mimeType: Future[MimeType] = Future(Resource.QuirrelData)
  def authorities: Future[Authorities] = db.authorities
  def close: Future[PrecogUnit] = db.close(as)
  def append(data: PathData) = data match {
    case NIHDBData(batch) =>
      db.insert(batch)

    case _ => Promise.failed(new IllegalArgumentException("Attempt to insert non-event data to NIHDB"))
  }
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

  def append(data: PathData): Future[PrecogUnit] = data match {
    case _ => Promise.failed(new IllegalArgumentException("Blob append not yet supported"))

    case BlobData(bytes, mimeType, _) => Future {
      if (mimeType != metadata.mimeType) {
        throw new IllegalArgumentException("Attempt to append %s data to a %s blob".format(mimeType, metadata.mimeType))
      }

      val output = new FileOutputStream(dataFile, true)
      output.write(bytes)
      output.close
    }

    case _ => Promise.failed(new IllegalArgumentException("Attempt to insert non-blob data to blob"))
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

sealed class PathData(val typeName: String)
object PathData {
  final val BLOB = "blob"
  final val NIHDB = "nihdb"
}

case class BlobData(data: Array[Byte], mimeType: MimeType, id: UUID) extends PathData(PathData.BLOB)
case class NIHDBData(data: Seq[(Long, Seq[JValue])], id: Option[UUID]) extends PathData(PathData.NIHDB)

sealed trait PathOp

/**
  * Creates a new resource with the given tracking id.
  */
case class Create(data: PathData, id: UUID, authorities: Authorities, failIfExists: Boolean) extends PathOp

/**
  * Appends to a specified resource or the current resource, depending on the id field.
  */
case class Append(data: PathData, authorities: Authorities) extends PathOp
case class Replace(id: UUID, auth: Option[APIKey]) extends PathOp
case class Read(auth: Option[APIKey]) extends PathOp
case class Execute(auth: Option[APIKey]) extends PathOp
case class Stat(auth: Option[APIKey]) extends PathOp


final case class VersionLog(versions: SortedMap[VersionId, String], current: VersionId = versions.foldLeft(0L)(math.max)) {
  def withCurrent(id: VersionId) =
    if (! versions.containsKey(id)) {
      sys.error("Cannot set current version to %d when it doesn't exist in the versions list: %s".format(id, versions))
    } else {
      VersionLog(versions, id)
    }

  def withCurrent(id: VersionId, resource: String) = {
    VersionLog(versions + (id -> resource), id)
  }

  // FIXME: Not thread safe. We're in an actor, so this should be OK,
  // but it would be better to use a cleaner approach
  def +(v: String): (VersionId, VersionLog) = {
    val newId = versions.foldLeft(0L)(math.max) + 1

    (newId, this + (newId -> v))
  }

  def +(kv: (VersionId, String)) = VersionLog(versions + kv)
}

object VersionLog {
  def empty = VersionLog(SortedMap.empty)

  implicit def extractor = new Extractor[VersionLog] {
    def validated(jvalue: JValue): Validation[Extractor.Error, VersionLog] = {
      for {
        current <- (jvalue \ "current").validated[Long]
        versions <- {
        (jvalue \ "versions") match {
          case JArray(elems) =>
            val versionsV: Validation[Extractor.Error, List[(Long, String)]] = elems.traverse { jobj =>
              (jobj.validated[Long] |@| jobj.validated[String]).tupled
            }
            versionsV map { versions => SortedMap(versions: _*) }

          case _ =>
            Failure(Extractor.invalid("Expected a JSON array in '.versions'"))
        }
      } yield {
          VersionLog(versions, current)
      }
    }
  }

  implicit def decomposer = new Decomposer[VersionLog] {
    def decompose(log: VersionLog): JValue =
      JObject(
        "current" -> JNum(log.current),
        "versions" ->
          JArray(log.versions.toList map { case (v, p) =>
            JObject(
              "version" -> JNum(v),
              "path" -> JString(p)
            )
          })
      )
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

  // Keeps track of the resources for a given version/authority pair
  private val resources = Cache.simple[VersionId, Map[Authorities, Resource]](
    OnRemoval {
      case (_, resourceMap, cause) if cause != RemovalCause.REPLACED => resourceMap.foreach(_.close)
    }) //,
    //ExpireAfterAccess(Duration(30, TimeUnit.Minutes))
  )

  // This tracks resource IDs to their versions. WE must guarantee
  // ordering of Create/Append/Replace elsewhere because we use that
  // guarantee to clean this on replacements
  private var resourceMap: Map[UUID, VersionId] = Map.empty()

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

  private def performCreate(data: PathData, id: Option[UUID], authorities: Authorities): IO[Validation[ResourceError, Resource]] = {
    if (resourceMap.containsKey(id)) {
      // What now?
    }

    for {
      version <- IO {
        val (newVersion, updatedVersions) = versions + data.typeName
        versions = updatedVersions
        newVersion
      }
      result <- data match {
        case BlobData(bytes, mimeType) =>
          resources.createBlob[IO](versionSubdir(version), mimeType, authorities, bytes :: StreamT.empty)

        case NIHDBData(data, idOpt) =>
          resources.createNIHDB(versionSubdir(version), authorities)
      }
      _ = result.map { resource =>
        id.foreach { i => resourceMap += (i -> version) }
        resources += (version -> Map(authorities -> resource))
      }
    } yield result
  }

  def receive = {
    case Create(data, id, authorities) =>
      val requestor = sender

      performCreate(data, id, authorities).map {
        case Success(resource) =>
          // Respond to requestor?

        case Failure(error) =>
          // ???
      }.unsafePerformIO

    case Append(data, authorities) =>
      val requestor = sender
      data match {
        case BlobData(_, _, id) => sys.error("Append not yet supported for blob data")

        case NIHDBData(data, idOpt) =>
          for {
            version <- idOpt.map { id =>
              resourceMap.get(id).toSuccess("Append for non-existing resource Id " + id)
            }.getOrElse(Success(versions.current))
            typeCheck <- 
          if (versions(version) != PathData.NIHDB) {
            // Can't append NIHDB data to a Blob
          } else {

          }


      id match {
        case Some(id) =>

        case None => performCreate(data, 

    case Read(auth) =>
      // Returns a stream of bytes + mimetype
    case Archive(auth) =>
      rollOver(identity)
    case Execute(auth) =>
      // Return projection snapshot if possible.
    case Stat(_) =>
      // Owners, a bit of history, mime type, etc.
  }
}
