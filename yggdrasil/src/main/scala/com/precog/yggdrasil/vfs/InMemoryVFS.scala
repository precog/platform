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

import ResourceError._
import table.Slice
import metadata.PathStructure

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.niflheim.Reductions
import com.precog.yggdrasil.actor.IngestData
import com.precog.yggdrasil.nihdb.NIHDBProjection
import com.precog.yggdrasil.vfs._
import com.precog.util._

import akka.dispatch.Future
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.json._
import blueeyes.util.Clock

import com.weiglewilczek.slf4s.Logging

import java.util.UUID

import scalaz._
import scalaz.EitherT._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.syntax.std.list._
import scalaz.effect.IO

trait InMemoryVFSModule[M[+_]] extends VFSModule[M, Slice] {
  //type Projection = ProjectionLike[M, Slice]

  object InMemoryVFS {
    sealed trait Record {
      def authorities: Authorities
      def versionId: UUID
    }

    case class BinaryRecord(data: Array[Byte], authorities: Authorities, versionId: UUID) extends Record
    case class JsonRecord(data: Vector[JValue], authorities: Authorities, versionId: UUID) extends Record

    private val vid = new java.util.concurrent.atomic.AtomicLong()

    private def newVersion: UUID = new UUID(0L, vid.getAndIncrement())
  }

  class InMemoryVFS(data0: Map[Path, (Array[Byte] \/ Vector[JValue], Authorities)], clock: Clock)(implicit M: Monad[M]) extends VFS {
    import InMemoryVFS._
    def toJsonElements(block: Slice) = block.toJsonElements

    private var data: Map[(Path, Version), Record] = data0 map { 
      case (p, (r, auth)) => (p, Version.Current) -> r.fold(BinaryRecord(_, auth, newVersion), JsonRecord(_, auth, newVersion))
    }

    def toResource(record: Record): M[Resource] = {
      sys.error("todo")
    }

    def writeAll(events: Seq[(Long, EventMessage)]): IO[PrecogUnit] = {
      def updated(acc: Map[(Path, Version), Record], appendTo: Option[Record], key: (Path, Version), writeAs: Authorities, values: Seq[JValue]) = {
        val path = key._1
        appendTo match {
          case Some(record @ BinaryRecord(v, auth, uuid)) =>
            acc + ((path, Version.Archived(uuid)) -> record) + ((path, Version.Current) -> JsonRecord(Vector(values: _*), writeAs, newVersion))

          case Some(JsonRecord(v, auth, uuid)) =>
            acc + (key -> JsonRecord(v ++ values, auth, uuid))

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
                  case JsonRecord(v, auth, `id`) =>
                    // append when it is the same id
                    acc + ((if (acc.contains(currentKey)) currentKey else archiveKey) -> JsonRecord(v ++ records.map(_.value), auth, id))

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
        case BinaryRecord(_, _, id) => VersionEntry(id, PathData.BLOB, clock.instant)
        case JsonRecord(_, _, id) => VersionEntry(id, PathData.NIHDB, clock.instant)
      }
    }

    def currentStructure(path: Path, selector: CPath): EitherT[M, ResourceError, PathStructure] = EitherT {
      M point {
        data.get((path, Version.Current)).toRightDisjunction(NotFound("No data found at path %s.".format(path.path))) flatMap {
          case JsonRecord(data, _, _) => 
            \/.right(sys.error("todo"))
          case _ => 
            \/.left(NotFound("Data at path %s is binary and thus has no JSON structure.".format(path.path)))
        }
      }
    }
  }
}
