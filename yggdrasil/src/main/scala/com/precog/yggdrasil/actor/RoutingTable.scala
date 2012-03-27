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
package actor

import com.precog.common._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import scala.collection.immutable.ListMap

case class ProjectionData(descriptor: ProjectionDescriptor, identities: Identities, values: Seq[CValue], metadata: Seq[Set[Metadata]])

trait RoutingTable {
  def route(msg: EventMessage): Array[ProjectionData]
}

class SingleColumnProjectionRoutingTable extends RoutingTable {
  
  final def route(msg: EventMessage): Array[ProjectionData] = {
    val pvs = msg.event.data.flattenWithPath

    val len = pvs.length

    val arr = new Array[ProjectionData](len)

    var cnt = 0
    while(cnt < len) {
      val pv = pvs(cnt)
      arr(cnt) = toProjectionData(msg, pv._1, pv._2)
      cnt += 1
    }
   
    arr
  }

  @inline
  private final def toProjectionData(msg: EventMessage, sel: JPath, value: JValue): ProjectionData = {
    val authorities = Set.empty + msg.event.tokenId
    val colDesc = ColumnDescriptor(msg.event.path, sel, CType.forValue(value).get, Authorities(authorities))

    val map = new ListMap[ColumnDescriptor, Int]() + (colDesc -> 0)
    val seq: Seq[(ColumnDescriptor, SortBy)] = (colDesc -> ById) :: Nil

    val projDesc = ProjectionDescriptor.trustedApply(1, map, seq)
    val identities = Vector1(msg.eventId.uid)
    val values = Vector1(CType.toCValue(value))
    val metadata = msg.event.metadata.get(sel).getOrElse(Set.empty).asInstanceOf[Set[Metadata]] :: Nil
    ProjectionData(projDesc, identities, values, metadata)
  }
}

trait ProjectionStorage {
  def store(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue])
}

trait MetadataStorage {
  def update(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]])
}
