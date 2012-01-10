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
package com.reportgrid.storage
package shard 

import blueeyes.json.JsonAST._

import com.reportgrid.common._

trait RoutingTable {
  def route(event: Set[(QualifiedSelector, JValue)]): Set[(ProjectionDescriptor, Seq[JValue])]
}

object RoutingTable {
  def unpack(e: Event): Set[Option[(BoundMetadata, JValue)]] = {
    e.content.map {
      case (sel, (jval, meta)) => 
        extract(jval).map { tnv => (BoundMetadata(QualifiedSelector(e.path, sel, tnv._1), meta), tnv._2) }
    }
  }

  implicit val lengthEncoder: LengthEncoder = null
  
  def extract(jval: JValue): Option[(PrimitiveType, JValue)] = ValueType.forValue(jval).map((_, jval))
}

class SingleColumnProjectionRoutingTable extends RoutingTable {
  def route(event: Set[(QualifiedSelector, JValue)]) = 
    event.map { t =>
      (ProjectionDescriptor(List(t._1), Set()), List(t._2))
    }
}

trait ProjectionStorage {
  def store(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue])
}

trait MetadataStorage {
  def update(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]])
}
