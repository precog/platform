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
package shard 

import com.precog.analytics._
import com.precog.common._

import blueeyes.json.JsonAST._

import scala.collection.immutable.ListMap

case class EventData(identity: Identity, event: Set[ColumnData])

case class ColumnData(descriptor: ColumnDescriptor, cvalue: CValue, metadata: Set[Metadata])

case class ProjectionData(descriptor: ProjectionDescriptor, identities: Identities, values: Seq[CValue])

trait RoutingTable {
  def route(eventData: EventData): Set[ProjectionData]
  
  // this is a candidate to factor out of the routing table
  // but will live here for now
  def route(msg: EventMessage): Set[ProjectionData] = {
    route(convertEventMessage(msg))
  }

  def convertEventMessage(msg: EventMessage): EventData
}

object RoutingTable {
  def unpack(e: Event): Set[ColumnData] = {
    e.content.map {
      case (sel, (jval, meta)) => 
        extract(jval).map { 
          case (ctype, data) => (ColumnDescriptor(e.path, sel, ctype, Ownership(Set())), data, meta) 
        }
    }
  }
  def extract(jval: JValue): Option[(ColumnType, JValue)] = ColumnType.forValue(jval).map((_, jval))
}

trait SingleColumnProjectionRoutingTable extends RoutingTable {

  def route(eventData: EventData) = {
    case EventData(identity, event) => event.map {
      case ColumnData(colDesc, cValue, metadata) => ProjectionData(toProjectionDescriptor(colDesc), List(identity), List(cValue))
    }
  }
  
  def toProjectionDescriptor(colDesc: ColumnDescriptor) = 
    ProjectionDescriptor(ListMap() + (colDesc -> 0), List[(ColumnDescriptor, SortBy)]() :+ (colDesc -> ById) ).toOption.get

}

object SingleColumnProjectionRoutingTable extends SingleColumnProjectionRoutingTable { }

trait ProjectionStorage {
  def store(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue])
}

trait MetadataStorage {
  def update(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]])
}
