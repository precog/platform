package com.precog.yggdrasil
package actor

import table._
import util.CPathUtils

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.json._

import blueeyes.json._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.ListMap

import scalaz.std.function._
import scalaz.std.stream._
import scalaz.syntax.arrow._
import scalaz.syntax.traverse._

trait RoutingTable {
  def routeIngest(msg: IngestMessage): Seq[ProjectionInsert]
  
  def routeArchive(msg: ArchiveMessage, descriptorMap: Map[Path, ProjectionDescriptor]): Seq[ProjectionArchive]

  def batchMessages(events: Seq[EventMessage], descriptorMap: Map[Path, ProjectionDescriptor]): Seq[ProjectionUpdate] = {
    // coalesce adjacent inserts into single inserts
    @tailrec def accumulate(updates: Stream[ProjectionUpdate], acc: Vector[ProjectionUpdate], last: Option[ProjectionInsert]): Vector[ProjectionUpdate] = {
      updates match {
        case (insert @ ProjectionInsert(descriptor, rows)) #:: xs => 
          accumulate(xs, acc, last map { i => ProjectionInsert(i.descriptor, i.rows ++ rows) } orElse Some(insert))

        case archive #:: xs => 
          accumulate(xs, acc ++ last :+ archive, None)

        case empty => 
          acc ++ last
      }
    }
    
    val projectionUpdates: Seq[ProjectionUpdate] = 
      for {
        event <- events
        projectionEvent <- event.fold[Seq[ProjectionUpdate]](routeIngest, routeArchive(_, descriptorMap))
      } yield projectionEvent

    // sequence the updates to interleave updates to the various projections; otherwise
    // each projection will get all of its updates at once. This may not really make
    // much of a difference.

    projectionUpdates.groupBy(_.descriptor).values.toStream.flatMap(g => accumulate(g.toStream, Vector(), None))
  }
}

class SinglePathProjectionRoutingTable extends RoutingTable {
  final def routeIngest(msg: IngestMessage): Seq[ProjectionInsert] = {
    val authorities = Authorities(Set(msg.ownerAccountId))
    val projDesc = ProjectionDescriptor(1, msg.path, authorities)
    val inserts = msg.data map { case IngestRecord(eventId, value) =>
      ProjectionInsert(projDesc, eventId, row :: Nil)
    }
    inserts
  }
  
  final def routeArchive(msg: ArchiveMessage, descriptorMap: Map[Path, Seq[ProjectionDescriptor]]): Seq[ProjectionArchive] = {
    descriptorMap.get(msg.archive.path).map { projDesc =>
      ProjectionArchive(projDesc, msg.eventId)
    }.toSeq
  }
}

// class SingleColumnProjectionRoutingTable extends RoutingTable {
//   final def routeIngest(msg: IngestMessage): Seq[ProjectionInsert] = {
//     val categorized = msg.data.foldLeft(Map.empty[(JPath, CType), Vector[ProjectionInsert.Row]]) {
//       case (acc, IngestRecord(eventId, jv)) =>
//         jv.flattenWithPath.foldLeft(acc) {
//           case (acc0, (selector, value)) => 
//             CType.forJValue(value) match { 
//               case Some(ctype) =>
//                 val key = (selector, ctype) 
//                 val row = ProjectionInsert.Row(eventId, List(CType.toCValue(value)), Nil)
//                 acc0 + (key -> (acc.getOrElse(key, Vector()) :+ row))
// 
//               case None =>
//                 // should never happen, since flattenWithPath only gives us the
//                 // leaf types and CType.forJValue is total in this set.
//                 sys.error("Could not determine ctype for ingest leaf " + value)
//             }
//         }
//     } 
// 
//     for (((selector, ctype), values) <- categorized.toStream) yield {
//       val colDesc = ColumnRef(msg.path, CPath(selector), ctype, Authorities(Set(msg.ownerAccountId)))
//       val projDesc = ProjectionDescriptor(1, List(colDesc))
// 
//       ProjectionInsert(projDesc, values)
//     }
//   }
// 
//   final def routeArchive(msg: ArchiveMessage, descriptorMap: Map[Path, Seq[ProjectionDescriptor]]): Seq[ProjectionArchive] = {
//     descriptorMap.get(msg.archive.path).flatten map { desc => ProjectionArchive(desc, msg.eventId) } toStream
//   }
// }
