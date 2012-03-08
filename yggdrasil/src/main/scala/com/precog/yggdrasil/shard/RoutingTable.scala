package com.precog.yggdrasil
package shard 

import com.precog.analytics._
import com.precog.common._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import scala.collection.immutable.ListMap

case class EventData(identity: Identity, event: Set[ColumnData])

case class ColumnData(descriptor: ColumnDescriptor, cvalue: CValue, metadata: Set[Metadata])

case class ProjectionData(descriptor: ProjectionDescriptor, identities: Identities, values: Seq[CValue], metadata: Seq[Set[Metadata]])

trait RoutingTable {
  def route(eventData: EventData): Array[ProjectionData]
  
  // this is a candidate to factor out of the routing table
  // but will live here for now
  def route(msg: EventMessage): Array[ProjectionData] = {
    route(convertEventMessage(msg))
  }

  def convertEventMessage(msg: EventMessage): EventData = EventData(msg.eventId.uid, unpack(msg.event))
  
  def unpack(e: Event): Set[ColumnData] = {
    def extract(jval: JValue): Option[(ColumnType, CValue)] = ColumnType.forValue(jval).map((_, convert(jval)))
    
    def convert(jval: JValue): CValue = jval match {
      case JString(s) => CString(s)
      case JInt(i) => CNum(BigDecimal(i))
      case JDouble(d) => CDouble(d)
      case JBool(b) => CBoolean(b)
      case JNull => CNull
      case _ => sys.error("unpossible")
    }

    e.data.flattenWithPath.flatMap {
      case (sel, jval) => extract(jval).map{
        case (ctype, cval) => 
          val colDesc = ColumnDescriptor(e.path, sel, ctype, Authorities(Set(e.tokenId)))
          val metadata: Set[Metadata] = e.metadata.get(sel).getOrElse(Set.empty).map(x => x)
          ColumnData(colDesc, cval, metadata)
      }
    }.toSet
  }
}

trait AltSingleColumnProjectionRoutingTable extends RoutingTable {
  
  final override def route(msg: EventMessage): Array[ProjectionData] = {
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

  def route(eventData: EventData): Array[ProjectionData] = Array()
  
  @inline
  private final def toCValue(jval: JValue): CValue = jval match {
    case JString(s) => CString(s)
    case JInt(i) => CNum(BigDecimal(i))
    case JDouble(d) => CDouble(d)
    case JBool(b) => CBoolean(b)
    case JNull => CNull
    case _ => sys.error("unpossible")
  }

  @inline
  private final def toProjectionData(msg: EventMessage, sel: JPath, value: JValue): ProjectionData = {
    val authorities = Set.empty + msg.event.tokenId
    val colDesc = ColumnDescriptor(msg.event.path, sel, ColumnType.forValue(value).get, Authorities(authorities))

    val map = new ListMap[ColumnDescriptor, Int]() + (colDesc -> 0)
    val seq: Seq[(ColumnDescriptor, SortBy)] = (colDesc -> ById) :: Nil

    val projDesc = ProjectionDescriptor.trustedApply(1, map, seq)
    val identities = Vector1(msg.eventId.uid)
    val values = Vector1(toCValue(value))
    val metadata = msg.event.metadata.get(sel).getOrElse(Set.empty).asInstanceOf[Set[Metadata]] :: Nil
    ProjectionData(projDesc, identities, values, metadata)
  }
}

object AltSingleColumnProjectionRoutingTable extends AltSingleColumnProjectionRoutingTable { }

trait SingleColumnProjectionRoutingTable extends RoutingTable {
  def route(eventData: EventData) = eventData match {
    case EventData(identity, event) => event.map {
      case ColumnData(colDesc, cValue, metadata) => 
        ProjectionData(toProjectionDescriptor(colDesc), Vector1(identity), Vector1(cValue), List(metadata))
    }.toArray
  }
  
  def toProjectionDescriptor(colDesc: ColumnDescriptor) = 
    ProjectionDescriptor(ListMap() + (colDesc -> 0), Seq[(ColumnDescriptor, SortBy)]() :+ (colDesc -> ById) ).toOption.get

}

object SingleColumnProjectionRoutingTable extends SingleColumnProjectionRoutingTable { }

trait ProjectionStorage {
  def store(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue])
}

trait MetadataStorage {
  def update(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]])
}
