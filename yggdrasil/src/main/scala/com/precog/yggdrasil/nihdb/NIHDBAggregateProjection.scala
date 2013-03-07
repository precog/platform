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

import akka.actor.ActorSystem
import akka.dispatch.Future

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security.Authorities
import com.precog.niflheim._
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.table._

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.traverse._

private[nihdb] class NIHDBAggregateProjection (underlying: NonEmptyList[NIHDBSnapshot])(implicit M: Monad[Future]) extends NIHDBProjection with Logging {
  private[this] val projectionId = NIHDBProjection.projectionIdGen.getAndIncrement
  private[this] val readers = underlying.list.map(_.readers).toArray.flatten

  def authorities: Future[Authorities] = sys.error("Authorities unsupported in aggregate")

  def getSnapshot(): Future[NIHDBSnapshot] = sys.error("Snapshot unsupported in aggregate")

  def getBlockAfter(id0: Option[Long], columns: Option[Set[ColumnRef]])(implicit M: Monad[Future]): Future[Option[BlockProjectionData[Long, Slice]]] = {
    M.point(
      try {
        // We're limiting ourselves to 2 billion blocks total here
        val index = id0.map(_.toInt).getOrElse(-1) + 1
        if (index >= readers.length) {
          None
        } else {
          val slice = SegmentsWrapper(readers(index).snapshot(columns.map(_.map(_.selector))).segments, projectionId, index)
          Some(BlockProjectionData(index, index, slice))
        }
      } catch {
        case e =>
          // Difficult to do anything else here other than bail
          logger.warn("Error during block read", e)
          None
      }
    )
  }

  def insert(v : Seq[IngestRecord])(implicit M: Monad[Future]): Future[PrecogUnit] = sys.error("Insert unsupported in aggregate")

  def length: Future[Long] = M.point(readers.map(_.length.toLong).sum)

  def structure: Future[Set[ColumnRef]] = M.point(readers.map(_.structure.map { case (s, t) => ColumnRef(s, t) }).toSet.flatten)

  def status: Future[Status] = sys.error("Status unsupported in aggregate")

  def commit: Future[PrecogUnit] = sys.error("Commit unsupported in aggregate")

  def close(implicit actorSystem: ActorSystem): Future[PrecogUnit] = M.point(PrecogUnit)
}
