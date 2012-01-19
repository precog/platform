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
          case (stype, metadata) => (ColumnDescriptor(QualifiedSelector(Path(e.path), sel, stype), meta), metadata) 
        }
    }
  }

  def extract(jval: JValue): Option[(SType, JValue)] = SType.forValue(jval).map((_, jval))
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
