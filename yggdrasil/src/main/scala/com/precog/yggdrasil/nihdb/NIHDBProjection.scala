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
package nihdb

import akka.dispatch.Future

import com.precog.common._
import com.precog.common.security.Authorities
import com.precog.niflheim._
import com.precog.yggdrasil.table.{SegmentsWrapper, Slice}
import com.precog.yggdrasil.vfs.NIHDBResource

import com.weiglewilczek.slf4s.Logging

import scalaz.{NonEmptyList => NEL, Monad, StreamT}

final class NIHDBProjection(snapshot: NIHDBSnapshot, val authorities: Authorities, projectionId: Int) extends ProjectionLike[Future, Long, Slice] with Logging {
  private[this] val readers = snapshot.readers
  val length = readers.map(_.length.toLong).sum

  override def toString = "NIHDBProjection(id = %d, len = %d, authorities = %s)".format(projectionId, length, authorities)

  def structure(implicit M: Monad[Future]) = M.point(readers.flatMap(_.structure)(collection.breakOut): Set[ColumnRef])

  def getBlockAfter(id0: Option[Long], columns: Option[Set[ColumnRef]])(implicit MP: Monad[Future]): Future[Option[BlockProjectionData[Long, Slice]]] = MP.point {
    val id = id0.map(_ + 1)
    val index = id getOrElse 0L
    getSnapshotBlock(id, columns.map(_.map(_.selector))) map {
      case Block(_, segments, _) =>
        val slice = SegmentsWrapper(segments, projectionId, index)
        BlockProjectionData(index, index, slice)
    }
  }

  def reduce[A](reduction: Reduction[A], path: CPath): Map[CType, A] = {
    readers.foldLeft(Map.empty[CType, A]) { (acc, reader) =>
      reader.snapshot(Some(Set(path))).segments.foldLeft(acc) { (acc, segment) =>
        reduction.reduce(segment, None) map { a =>
          val key = segment.ctype
          val value = acc.get(key).map(reduction.semigroup.append(_, a)).getOrElse(a)
          acc + (key -> value)
        } getOrElse acc
      }
    }
  }

  private def getSnapshotBlock(id: Option[Long], columns: Option[Set[CPath]]): Option[Block] = {
    try {
      // We're limiting ourselves to 2 billion blocks total here
      val index = id.map(_.toInt).getOrElse(0)
      if (index >= readers.length) {
        None
      } else {
        Some(Block(index, readers(index).snapshot(columns).segments, true))
      }
    } catch {
      case e =>
        // Difficult to do anything else here other than bail
        logger.warn("Error during block read", e)
        None
    }
  }
}

object NIHDBProjection {
  def wrap(nihdb: NIHDB, authorities: Authorities)(implicit M: Monad[Future]): Future[NIHDBProjection] = nihdb.getSnapshot map { snap =>
    new NIHDBProjection(snap, authorities, nihdb.projectionId)
  }

  def wrap(resource: NIHDBResource)(implicit M: Monad[Future]): Future[NIHDBProjection] =
    wrap(resource.db, resource.authorities)
}
