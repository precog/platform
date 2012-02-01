package com.precog.yggdrasil
package shard 

import com.precog.analytics._
import com.precog.common._

import blueeyes.json.JsonAST._

import scala.collection.immutable.ListMap

trait RoutingTable {
  def route(event: Set[(ColumnDescriptor, JValue)]): Set[(ProjectionDescriptor, Seq[JValue])]
}

object RoutingTable {
  def unpack(e: Event): Set[Option[(ColumnDescriptor, JValue, Set[Metadata])]] = {
    e.content.map {
      case (sel, (jval, meta)) => 
        extract(jval).map { 
          case (ctype, data) => (ColumnDescriptor(e.path, sel, ctype, Ownership(Set())), data, meta) 
        }
    }
  }

  def extract(jval: JValue): Option[(ColumnType, JValue)] = ColumnType.forValue(jval).map((_, jval))
}

class SingleColumnProjectionRoutingTable extends RoutingTable {
  def route(event: Set[(ColumnDescriptor, JValue)]) = {
    
    def toProjectionDescriptor(colDesc: ColumnDescriptor) = 
      ProjectionDescriptor(ListMap() + (colDesc -> 0), List[(ColumnDescriptor, SortBy)]() :+ (colDesc -> ById) ).toOption.get

    event.map {
      case (colDesc, jvalue) => 
        (toProjectionDescriptor(colDesc), List(jvalue))
    }
  }
}

trait ProjectionStorage {
  def store(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue])
}

trait MetadataStorage {
  def update(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]])
}
