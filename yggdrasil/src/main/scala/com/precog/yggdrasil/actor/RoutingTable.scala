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
package actor

import util.CPathUtils

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.json._

import blueeyes.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.ListMap

import scalaz.std.function._
import scalaz.std.stream._
import scalaz.syntax.arrow._
import scalaz.syntax.traverse._

trait RoutingTable {
  def routeIngest(msg: IngestMessage): Seq[ProjectionInsert]
  
  def routeArchive(msg: ArchiveMessage, descriptorMap: Map[Path, Seq[ProjectionDescriptor]]): Seq[ProjectionArchive]

  def batchMessages(events: Seq[EventMessage], descriptorMap: Map[Path, Seq[ProjectionDescriptor]]): Seq[ProjectionUpdate] = {
    // coalesce adjacent inserts into single inserts
    @tailrec def accumulate(updates: List[ProjectionUpdate], acc: Vector[ProjectionUpdate], last: Option[ProjectionInsert]): Vector[ProjectionUpdate] = {
      updates match {
        case (insert @ ProjectionInsert(descriptor, rows)) :: xs => 
          accumulate(xs, acc, last map { i => ProjectionInsert(i.descriptor, i.rows ++ rows) } orElse Some(insert))

        case archive :: xs => 
          accumulate(xs, acc ++ last :+ archive, None)

        case Nil => 
          acc ++ last
      }
    }
    
    val projectionUpdates: Seq[ProjectionUpdate] = 
      for {
        event <- events
        projectionEvent <- event.fold(routeIngest, routeArchive(_, descriptorMap))
      } yield projectionEvent

    // sequence the updates to interleave updates to the various projections; otherwise
    // each projection will get all of its updates at once. This may not really make
    // much of a difference.
    projectionUpdates.groupBy(_.descriptor).values map { group: List[ProjectionUpdate] => 
      accumulate(group.toList, Vector(), None)
    }.flatten
  }
}


class SingleColumnProjectionRoutingTable extends RoutingTable {
  final def routeIngest(msg: IngestMessage): Seq[ProjectionInsert] = {
    val categorized = msg.data.foldLeft(Map.empty[(JPath, CType), Vector[CValue]]) {
      case (acc, IngestRecord(eventId, jv)) =>
        jv.flattenWithPath.foldLeft(acc) {
          case (acc0, (selector, value)) => 
            CType.forJValue(value) match { 
              case Some(ctype) =>
                val key = (selector, ctype) 
                acc0 + (key -> (acc.getOrElse(key, Vector()) :+ CType.toCValue(value)))
              case None =>
                // should never happen, since flattenWithPath only gives us the
                // leaf types and CType.forJValue is total in this set.
                sys.error("Could not determine ctype for ingest leaf " + value)
            }
        }
    } 

    for {
      ((selector, ctype), values) <- categorized
    } yield {
      val owner = msg.ownerAccountId getOrElse {
        // TODO: This check is a bit too far down in the stack; by the time an event
        // gets to routing it should have all the information it needs associated with it.
        sys.error("Cannot store event without an owner account id: " + msg)
      }

      val colDesc = ColumnDescriptor(msg.path, selector, ctype, Authorities(Set(owner)))
      toProjectionData(msg.ownerAccountId.get, CPath(selector), ctype, values)
    }
  }

  final def routeArchive(msg: ArchiveMessage, descriptorMap: Map[Path, Seq[ProjectionDescriptor]]): Seq[ProjectionArchive] = {
    descriptorMap.get(msg.archive.path).flatten map { desc => ProjectionArchive(desc, msg.eventId) } toStream
  }
  
  @inline
  private final def toProjectionData(owner: AccountId, path: Path, selector: CPath, ctype: CType, values: Seq[JValue]): ProjectionInsert = {
    val projDesc = ProjectionDescriptor(1, List(colDesc))

    val cvalues = values map CType.toCValue 

    ProjectionInsert(projDesc, cvalues, Nil)
  }
}
