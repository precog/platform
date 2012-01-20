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
package com.reportgrid.yggdrasil
package shard 

import com.reportgrid.analytics._
import com.reportgrid.common._

import blueeyes.json.JsonAST._

trait RoutingTable {
  def route(event: Set[(QualifiedSelector, JValue)]): Set[(ProjectionDescriptor, Seq[JValue])]
}

object RoutingTable {
  def unpack(e: Event): Set[Option[(ColumnDescriptor, JValue)]] = {
    e.content.map {
      case (sel, (jval, meta)) => 
        extract(jval).map { 
          case (ctype, metadata) => (ColumnDescriptor(QualifiedSelector(Path(e.path), sel, ctype), meta), metadata) 
        }
    }
  }

  def extract(jval: JValue): Option[(ColumnType, JValue)] = ColumnType.forValue(jval).map((_, jval))
}

class SingleColumnProjectionRoutingTable extends RoutingTable {
  def route(event: Set[(QualifiedSelector, JValue)]) = 
    event.map {
      case (selector, jvalue) => 
        sys.error("todo")
        //(ProjectionDescriptor(List(selector), Set()), List(jvalue))
    }
}

trait ProjectionStorage {
  def store(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue])
}

trait MetadataStorage {
  def update(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]])
}
