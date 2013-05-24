package com.precog.yggdrasil
package vfs

import ResourceError._
import table.Slice
import metadata.PathStructure

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.niflheim._
import com.precog.yggdrasil.actor.IngestData
import com.precog.yggdrasil.nihdb.NIHDBProjection
import com.precog.yggdrasil.vfs._
import com.precog.util._

import akka.dispatch.Future
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.json._
import blueeyes.core.http.MimeType
import blueeyes.util.Clock

import com.weiglewilczek.slf4s.Logging

import java.util.UUID

import scalaz._
import scalaz.EitherT._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.std.option._
import scalaz.effect.IO

trait InMemoryVFSModule[M[+_]] extends VFSModule[M, Slice] { moduleSelf =>
  class Projection(slices0: Vector[Slice]) extends ProjectionLike[M, Slice] {
    type Key = Int

    @volatile private var slices = slices0
    private[InMemoryVFSModule] def append(slice: Slice) = synchronized { slices = slices :+ slice }

    def structure(implicit M: Monad[M]): M[Set[ColumnRef]] = M.point(slices.flatMap(_.columns.keySet).toSet)
    def length: Long = slices.map(_.size).sum.toLong
    def getBlockAfter(id: Option[Key], columns: Option[Set[ColumnRef]] = None)(implicit M: Monad[M]): M[Option[BlockProjectionData[Key, Slice]]] = M point {
      id match {
        case Some(i) => slices.lift(i+1) map { s => BlockProjectionData(i+1, i+1, s) }
        case None => slices.headOption map { s => BlockProjectionData(0, 0, s) }
      }
    }
  }

  case class InMemoryProjectionResource(proj: Projection, authorities: Authorities) extends ProjectionResource {
    val mimeType = FileContent.XQuirrelData

    def append(data: NIHDB.Batch): IO[PrecogUnit] = IO {
      proj.append(Slice.fromJValues(data.values.toStream))
    }

    def recordCount(implicit M: Monad[M]): M[Long] = M point { proj.length }
    def projection(implicit M: Monad[M]): M[Projection] = M point { proj }
    def byteStream(matchType: Option[MimeType])(implicit M: Monad[M]): OptionT[M, StreamT[M, Array[Byte]]] = OptionT(M point {
      table.ColumnarTableModule.byteStream(proj.getBlockStream(None), matchType)
    })
  }

  case class InMemoryBlobResource(data: Array[Byte], mimeType: MimeType, authorities: Authorities) extends BlobResource {
    def byteLength = data.length.toLong

    def asString(implicit M: Monad[M]): OptionT[M, String] = OptionT(M point {
      FileContent.stringTypes.contains(mimeType).option(new String(data, "UTF-8"))
    })

    def byteStream(matchType: Option[MimeType])(implicit M: Monad[M]): OptionT[M, StreamT[M, Array[Byte]]] = OptionT(M point {
      matchType.forall(_ == mimeType).option(data :: StreamT.empty[M, Array[Byte]])
    })
  }

  object InMemoryVFS {
    sealed trait Record {
      def resource: Resource
      def versionId: UUID

      def authorities: Authorities = resource.authorities
    }

    case class BinaryRecord(resource: InMemoryBlobResource, versionId: UUID) extends Record
    object BinaryRecord {
      def apply(data: (Array[Byte], MimeType), authorities: Authorities, uuid: UUID) = 
        new BinaryRecord(new InMemoryBlobResource(data._1, data._2, authorities), uuid)
    }

    case class JsonRecord(resource: InMemoryProjectionResource, versionId: UUID) extends Record
    object JsonRecord {
      def apply(data: Vector[JValue], authorities: Authorities, uuid: UUID) = 
        new JsonRecord(new InMemoryProjectionResource(new Projection(Vector(Slice.fromJValues(data.toStream))), authorities), uuid)
    }

    private val vid = new java.util.concurrent.atomic.AtomicLong()

    private def newVersion: UUID = new UUID(0L, vid.getAndIncrement())
  }

  class InMemoryVFS(data0: Map[Path, ((Array[Byte], MimeType) \/ Vector[JValue], Authorities)], clock: Clock)(implicit M: Monad[M]) extends VFS {
    import InMemoryVFS._
    def toJsonElements(block: Slice) = block.toJsonElements

    private var data: Map[(Path, Version), Record] = data0 map { 
      case (p, (r, auth)) => 
        (p, Version.Current) -> r.fold(
          BinaryRecord(_, auth, newVersion), 
          JsonRecord(_, auth, newVersion)
        )
    }

    def toResource(record: Record): M[Resource] = M point { record.resource }

    def writeAll(events: Seq[(Long, EventMessage)]): IO[PrecogUnit] = {
      def updated(acc: Map[(Path, Version), Record], appendTo: Option[Record], key: (Path, Version), writeAs: Authorities, values: Seq[JValue]) = {
        val path = key._1
        appendTo match {
          case Some(record @ BinaryRecord(resource, uuid)) =>
            acc + ((path, Version.Archived(uuid)) -> record) + ((path, Version.Current) -> JsonRecord(Vector(values: _*), writeAs, newVersion))

          case Some(rec @ JsonRecord(resource, _)) =>
            //TODO: fix the ugly
            rec.resource.append(NIHDB.Batch(0, values)).unsafePerformIO
            acc + (key -> rec)

          case None =>
            // TODO: no permissions checking here (create required)
            acc + (key -> JsonRecord(Vector(values: _*), writeAs, newVersion))
        }
      }

      IO {
        data = (events groupBy { case (offset, msg) => msg.path }).foldLeft(data) {
          case (acc, (path, messages)) => 
            val currentKey = (path, Version.Current)
            // We can discard the event IDs for the purposes of this class
            messages.map(_._2).foldLeft(acc) {
              case (acc, IngestMessage(_, _, writeAs, records, _, _, StreamRef.Append)) =>
                updated(acc, acc.get(currentKey), currentKey, writeAs, records.map(_.value))

              case (acc, IngestMessage(_, _, writeAs, records, _, _, StreamRef.Create(id, _))) =>
                val archiveKey = (path, Version.Archived(id))
                val appendTo = acc.get(archiveKey).orElse(acc.get(currentKey).filter(_.versionId == id))
                updated(acc, appendTo, if (acc.contains(currentKey)) currentKey else archiveKey, writeAs, records.map(_.value))

              case (acc, IngestMessage(_, _, writeAs, records, _, _, StreamRef.Replace(id, _))) =>
                val archiveKey = (path, Version.Archived(id))
                acc.get(archiveKey).orElse(acc.get(currentKey)) map {
                  case rec @ JsonRecord(resource, `id`) =>
                    // append when it is the same id
                    rec.resource.append(NIHDB.Batch(0, records.map(_.value))).unsafePerformIO
                    acc + ((if (acc.contains(currentKey)) currentKey else archiveKey) -> rec)

                  case record =>
                    // replace when id is not recognized, or when record is binary
                    acc + ((path, Version.Archived(record.versionId)) -> record) + (currentKey -> JsonRecord(Vector(records.map(_.value): _*), writeAs, id))
                } getOrElse {
                  // start a new current version
                  acc + (currentKey -> JsonRecord(Vector(records.map(_.value): _*), writeAs, id))
                }

              case (acc, StoreFileMessage(_, _, writeAs, _, _, content, _, StreamRef.Create(id, _))) =>
                sys.error("todo")

              case (acc, StoreFileMessage(_, _, writeAs, _, _, content, _, StreamRef.Replace(id, _))) =>
                sys.error("todo")

              case (acc, _: ArchiveMessage) =>
                acc ++ acc.get(currentKey).map(record => (path, Version.Archived(record.versionId)) -> record)
            }
        }
      }
    }

    def writeAllSync(events: Seq[(Long, EventMessage)]): EitherT[M, ResourceError, PrecogUnit] = {
      EitherT.right(M.point(writeAll(events).unsafePerformIO))
    }

    def readResource(path: Path, version: Version): EitherT[M, ResourceError, Resource] = {
      EitherT {
        data.get((path, version)).toRightDisjunction(NotFound("No data found for path %s version %s".format(path.path, version))) traverse { toResource }
      }
    }

    def findDirectChildren(path: Path): M[Set[Path]] = M point {
      data.keySet.map(_._1) flatMap { _ - path }
    }

    def currentVersion(path: Path): M[Option[VersionEntry]] = M point {
      data.get((path, Version.Current)) map { 
        case BinaryRecord(_, id) => VersionEntry(id, PathData.BLOB, clock.instant)
        case JsonRecord(_, id) => VersionEntry(id, PathData.NIHDB, clock.instant)
      }
    }

    def pathStructure(path: Path, selector: CPath, version: Version): EitherT[M, ResourceError, PathStructure] = {
      readProjection(path, version) flatMap { projection =>
        EitherT.right(
          for (columnRefs <- projection.structure) yield { 
            val types : Map[CType, Long] = columnRefs.collect {
              // FIXME: This should use real counts
              case ColumnRef(selector, ctype) if selector.hasPrefix(selector) => (ctype, 0L)
            }.groupBy(_._1).map { case (tpe, values) => (tpe, values.map(_._2).sum) }

            PathStructure(types, columnRefs.map(_.selector))
          }
        )
      }
    }
  }
}
