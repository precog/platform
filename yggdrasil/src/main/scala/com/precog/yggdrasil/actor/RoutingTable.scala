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

    val projDesc = ProjectionDescriptor(1, List(colDesc))
    val identities = Vector1(msg.eventId.uid)
    val values = Vector1(CType.toCValue(value))
    val metadata = msg.event.metadata.get(sel).getOrElse(Set.empty).asInstanceOf[Set[Metadata]] :: Nil
    ProjectionData(projDesc, identities, values, metadata)
  }
}
