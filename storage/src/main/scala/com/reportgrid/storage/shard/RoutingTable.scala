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
