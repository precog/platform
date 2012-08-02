package com.precog.yggdrasil
package actor

import com.precog.common._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.ListMap

case class ProjectionData(descriptor: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]) {
  def toJValue: JValue = {
    assert(descriptor.columns.size == values.size)
    (descriptor.columns zip values).foldLeft[JValue](JObject(Nil)) {
      case (acc, (colDesc, cv)) => 
        acc.set(colDesc.selector, cv.toJValue)
    }
  }
}

trait RoutingTable {
  def route(msg: EventMessage): Seq[ProjectionData]

  def batchMessages(events: Iterable[IngestMessage]): Seq[ProjectionInsert] = {
    import ProjectionInsert.Row

    val actions = mutable.Map.empty[ProjectionDescriptor, Seq[Row]]

    @inline @tailrec
    def update(eventId: EventId, updates: Iterator[ProjectionData]): Unit = {
      if (updates.hasNext) {
        val ProjectionData(descriptor, values, metadata) = updates.next()
        val insert = Row(eventId, values, metadata)
        
        actions += (descriptor -> (actions.getOrElse(descriptor, Vector.empty) :+ insert))
        update(eventId, updates)
      } 
    }

    @inline @tailrec
    def build(events: Iterator[IngestMessage]): Unit = {
      if (events.hasNext) {
        events.next() match {
          case em @ EventMessage(eventId, _) => update(eventId, route(em).iterator)
        }

        build(events)
      }
    }

    build(events.iterator);
    actions.map({ case (descriptor, rows) => ProjectionInsert(descriptor, rows) })(collection.breakOut)
  }
}


class SingleColumnProjectionRoutingTable extends RoutingTable {
  final def route(msg: EventMessage): List[ProjectionData] = {
    msg.event.data.flattenWithPath map { 
      case (selector, value) => toProjectionData(msg, selector, value)
    }
  }

  @inline
  private final def toProjectionData(msg: EventMessage, selector: JPath, value: JValue): ProjectionData = {
    val authorities = Set.empty + msg.event.tokenId
    val colDesc = ColumnDescriptor(msg.event.path, selector, CType.forJValue(value).get, Authorities(authorities))

    val projDesc = ProjectionDescriptor(1, List(colDesc))

    val values = Vector1(CType.toCValue(value))
    val metadata = msg.event.metadata.get(selector).getOrElse(Set.empty).asInstanceOf[Set[Metadata]] :: Nil

    ProjectionData(projDesc, values, metadata)
  }
}
