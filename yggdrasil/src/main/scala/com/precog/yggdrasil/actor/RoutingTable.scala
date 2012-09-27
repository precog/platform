package com.precog.yggdrasil
package actor

import util.CPathUtils

import com.precog.common.json._
import com.precog.common._

import blueeyes.json.JsonAST._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.ListMap

case class ProjectionData(descriptor: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]) {
  def toJValue: JValue = {
    assert(descriptor.columns.size == values.size)
    (descriptor.columns zip values).foldLeft[JValue](JObject(Nil)) {
      case (acc, (colDesc, cv)) => {
        CPathUtils.cPathToJPaths(colDesc.selector, cv).foldLeft(acc) {
          case (acc, (path, value)) => acc.set(path, value.toJValue)
        }
      }
    }
  }
}

case class ArchiveData(descriptor: ProjectionDescriptor)

trait RoutingTable {
  def routeEvent(msg: EventMessage): Seq[ProjectionData]
  
  def routeArchive(msg: ArchiveMessage, descriptorMap: Map[Path, Seq[ProjectionDescriptor]]): Seq[ArchiveData]

  def batchMessages(events: Iterable[IngestMessage], descriptorMap: Map[Path, Seq[ProjectionDescriptor]]): Seq[ProjectionUpdate] = {
    import ProjectionInsert.Row
    
    val updates = events.flatMap {
      case em @ EventMessage(eventId, _) => routeEvent(em).map {
        case ProjectionData(descriptor, values, metadata) => (descriptor, Row(eventId, values, metadata)) 
      }
      
      case am @ ArchiveMessage(archiveId, _) => routeArchive(am, descriptorMap).map { 
        case ArchiveData(descriptor) => (descriptor, archiveId)
      }
    }
    
    val grouped = updates.groupBy(_._1).mapValues(_.map(_._2))
    
    val revBatched = grouped.flatMap {
      case (desc, updates) => updates.foldLeft(List.empty[ProjectionUpdate]) {
        case (rest, id : ArchiveId) => ProjectionArchive(desc, id) :: rest
        case (ProjectionInsert(_, inserts) :: rest, insert : Row) => ProjectionInsert(desc, insert +: inserts) :: rest
        case (rest, insert : Row) => ProjectionInsert(desc, Seq(insert)) :: rest
      }
    }
    
    revBatched.map {
      case ProjectionInsert(desc, inserts) => ProjectionInsert(desc, inserts.reverse)
      case archive => archive
    }.toSeq.reverse
  }
}


class SingleColumnProjectionRoutingTable extends RoutingTable {
  final def routeEvent(msg: EventMessage): Seq[ProjectionData] = {
    msg.event.data.flattenWithPath map { 
      case (selector, value) => toProjectionData(msg, CPath(selector), value)
    }
  }

  final def routeArchive(msg: ArchiveMessage, descriptorMap: Map[Path, Seq[ProjectionDescriptor]]): Seq[ArchiveData] = {
    (for {
      desc <- descriptorMap.get(msg.archive.path).flatten 
    } yield ArchiveData(desc))(collection.breakOut)
  }
  
  @inline
  private final def toProjectionData(msg: EventMessage, selector: CPath, value: JValue): ProjectionData = {
    val authorities = Set.empty + msg.event.tokenId
    val colDesc = ColumnDescriptor(msg.event.path, selector, CType.forJValue(value).get, Authorities(authorities))

    val projDesc = ProjectionDescriptor(1, List(colDesc))

    val values = Vector1(CType.toCValue(value))
    val metadata: List[Set[Metadata]] = CPathUtils.cPathToJPaths(selector, values.head) map {
      case (path, _) => 
        msg.event.metadata.get(path).getOrElse(Set()).asInstanceOf[Set[Metadata]]       // and now I hate my life...
    }

    ProjectionData(projDesc, values, metadata)
  }
}
