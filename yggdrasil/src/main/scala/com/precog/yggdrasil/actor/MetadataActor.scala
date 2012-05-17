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
package com.precog.yggdrasil
package actor 

import metadata._

import com.precog.common._
import com.precog.common.util._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import blueeyes.json.xschema.Decomposer
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.dispatch.MessageDispatcher
import akka.dispatch.Future

import scalaz.Scalaz._

object MetadataActor {
  case class State(projections: Map[ProjectionDescriptor, ColumnMetadata], dirty: Set[ProjectionDescriptor], clock: VectorClock) {
    def toSaveMessage = SaveMetadata(projections.filterKeys(dirty), clock)
  }
}

class MetadataActor(private var state: MetadataActor.State) extends Actor { metadataActor =>
  import MetadataActor._
  import ProjectionMetadata._

  def receive = {
    case Status => sender ! status

    case IngestBatchMetadata(patch, checkpoint) => 
      state = State(state.projections |+| patch, state.dirty ++ patch.keySet, state.clock max checkpoint.messageClock)
   
    case FindChildren(path)                   => sender ! findChildren(path)
    
    case FindSelectors(path)                  => sender ! findSelectors(path)

    case FindDescriptors(path, selector)      => sender ! findDescriptors(path, selector)

    case FindPathMetadata(path, selector)     => sender ! findPathMetadata(path, selector)
    
    case FlushMetadata(serializationActor)    => serializationActor ! state.toSaveMessage
  }

  def status: JValue = JObject(JField("Metadata", JObject(JField("state", JString(state.toString)) :: Nil)) :: Nil)

  private def isChildPath(ref: Path, test: Path): Boolean = 
    test.elements.startsWith(ref.elements) && 
    test.elements.size > ref.elements.size

  def findChildren(path: Path): Set[Path] = 
    state.projections.foldLeft(Set[Path]()) {
      case (acc, (descriptor, _)) => 
        acc ++ descriptor.columns.collect { 
          case ColumnDescriptor(cpath, cselector, _, _) if isChildPath(path, cpath) => Path(cpath.elements(path.elements.length))
        }
    }
 
  def findSelectors(path: Path): Seq[JPath] = 
    state.projections.foldLeft(Vector[JPath]()) {
      case (acc, (descriptor, _)) => 
        acc ++ descriptor.columns.collect { 
          case ColumnDescriptor(cpath, cselector, _, _) if path == cpath => cselector 
        }
    }

  def findDescriptors(path: Path, selector: JPath): Map[ProjectionDescriptor, ColumnMetadata] = {
    @inline def isEqualOrChild(ref: JPath, test: JPath) = test.nodes startsWith ref.nodes

    @inline def matches(path: Path, selector: JPath) = (col: ColumnDescriptor) => {
      col.path == path && isEqualOrChild(selector, col.selector)
    }

    state.projections.collect {
      case (descriptor, cm) if (descriptor.columns.exists(matches(path, selector))) => (descriptor -> cm)
    }
  } 

  case class ResolvedSelector(selector: JPath, authorities: Authorities, descriptor: ProjectionDescriptor, metadata: ColumnMetadata) {
    def columnType: CType = descriptor.columns.find(_.selector == selector).map(_.valueType).get
  }
  
  @inline def isEqualOrChild(ref: JPath, test: JPath) = test.nodes startsWith ref.nodes

  @inline def matches(path: Path, selector: JPath) = (col: ColumnDescriptor) => {
    col.path == path && isEqualOrChild(selector, col.selector)
  }

  @inline def matching(path: Path, selector: JPath): Seq[ResolvedSelector] = 
    state.projections.flatMap {
      case (desc, meta) => desc.columns.collect {
        case col @ ColumnDescriptor(_,sel, _,auth) if matches(path,selector)(col) => ResolvedSelector(sel, auth, desc, meta)
      }
    }(collection.breakOut)

  def findPathMetadata(path: Path, selector: JPath): PathRoot = {
    @inline def isLeaf(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length - 1 == ref.nodes.length
    }
    
    @inline def isObjectBranch(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length > ref.nodes.length &&
      (test.nodes(ref.nodes.length) match {
        case JPathField(_) => true
        case _             => false
      })
    }
    
    @inline def isArrayBranch(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length > ref.nodes.length &&
      (test.nodes(ref.nodes.length) match {
        case JPathIndex(_) => true
        case _             => false
      })
    }

    def extractIndex(base: JPath, child: JPath): Int = child.nodes(base.length) match {
      case JPathIndex(i) => i
      case _             => sys.error("assertion failed") 
    }

    def extractName(base: JPath, child: JPath): String = child.nodes(base.length) match {
      case JPathField(n) => n 
      case _             => sys.error("unpossible")
    }

    def newIsLeaf(ref: JPath, test: JPath): Boolean = ref == test 

    def selectorPartition(sel: JPath, rss: Seq[ResolvedSelector]):
        (Seq[ResolvedSelector], Seq[ResolvedSelector], Set[Int], Set[String]) = {
      val (values, nonValues) = rss.partition(rs => newIsLeaf(sel, rs.selector))
      val (indexes, fields) = rss.foldLeft( (Set.empty[Int], Set.empty[String]) ) {
        case (acc @ (is, fs), rs) => if(isArrayBranch(sel, rs.selector)) {
          (is + extractIndex(sel, rs.selector), fs)
        } else if(isObjectBranch(sel, rs.selector)) {
          (is, fs + extractName(sel, rs.selector))
        } else {
          acc
        }
      }

      (values, nonValues, indexes, fields)
    }

    def convertValues(values: Seq[ResolvedSelector]): Set[PathMetadata] = {
      values.foldLeft(Map[(JPath, CType), (Authorities, Map[ProjectionDescriptor, ColumnMetadata])]()) {
        case (acc, rs @ ResolvedSelector(sel, auth, desc, meta)) => 
          val key = (sel, rs.columnType)
          val update = acc.get(key).map(_._2).getOrElse( Map.empty[ProjectionDescriptor, ColumnMetadata] ) + (desc -> meta)
          acc + (key -> (auth, update))
      }.map {
        case ((sel, colType), (auth, meta)) => PathValue(colType, auth, meta) 
      }(collection.breakOut)
    }

    def buildTree(branch: JPath, rs: Seq[ResolvedSelector]): Set[PathMetadata] = {
      val (values, nonValues, indexes, fields) = selectorPartition(branch, rs)

      val oval = convertValues(values)
      val oidx = indexes.map { idx => PathIndex(idx, buildTree(branch \ idx, nonValues)) }
      val ofld = fields.map { fld => PathField(fld, buildTree(branch \ fld, nonValues)) }

      oval ++ oidx ++ ofld
    }

    PathRoot(buildTree(selector, matching(path, selector)))
  }

/*
  def toStorageMetadata(messageDispatcher: MessageDispatcher): StorageMetadata = new StorageMetadata {
    implicit val dispatcher = messageDispatcher 

    def update(inserts: Seq[InsertComplete]) = Future {
      metadataActor.update(inserts)
    }

    def findChildren(path: Path) = Future {
      metadataActor.findChildren(path)
    }

    def findSelectors(path: Path) = Future {
      metadataActor.findSelectors(path) 
    }

    def findProjections(path: Path, selector: JPath) = Future {
      metadataActor.findDescriptors(path, selector)
    } 

    def findPathMetadata(path: Path, selector: JPath) = Future {
      metadataActor.findPathMetadata(path, selector)
    }
  }
*/
}

object ProjectionMetadata {
  import metadata._
  import ProjectionInsert.Row

  def columnMetadata(desc: ProjectionDescriptor, rows: Seq[Row]): ColumnMetadata = {
    rows.foldLeft(ColumnMetadata.Empty) { 
      case (acc, Row(_, values, metadata)) => acc |+| columnMetadata(desc, values, metadata) 
    }
  }

  def columnMetadata(desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]): ColumnMetadata = {
    def addValueMetadata(values: Seq[CValue], metadata: Seq[MetadataMap]): Seq[MetadataMap] = {
      values zip metadata map { case (value, mmap) => valueStats(value).map(vs => mmap + (vs.metadataType -> vs)).getOrElse(mmap) }
    }

    val userAndValueMetadata: Seq[MetadataMap] = addValueMetadata(values, metadata.map { Metadata.toTypedMap _ })
    (desc.columns zip userAndValueMetadata).toMap
  }

  def updateColumnMetadata(initialMetadata: ColumnMetadata, desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]): ColumnMetadata = {
    columnMetadata(desc, values, metadata).foldLeft(initialMetadata) { 
      case (acc, (col, newColMetadata)) =>
        val updatedMetadata = acc.get(col) map { _ |+| newColMetadata } getOrElse { newColMetadata }

        acc + (col -> updatedMetadata)
    }
  }

  def initMetadata(desc: ProjectionDescriptor): ColumnMetadata = {
    desc.columns.foldLeft( Map[ColumnDescriptor, MetadataMap]() ) {
      (acc, col) => acc + (col -> Map[MetadataType, Metadata]())
    }
  }

  def valueStats(cval: CValue): Option[Metadata] = cval match { 
    case CString(s)  => Some(StringValueStats(1, s, s))
    case CBoolean(b) => Some(BooleanValueStats(1, if(b) 1 else 0))
    case CInt(i)     => Some(LongValueStats(1, i, i))
    case CLong(l)    => Some(LongValueStats(1, l, l))
    case CFloat(f)   => Some(DoubleValueStats(1, f, f))
    case CDouble(d)  => Some(DoubleValueStats(1, d, d))
    case CNum(bd)    => Some(BigDecimalValueStats(1, bd, bd))
    case _           => None
  }
}   

sealed trait ShardMetadataAction

case class ExpectedEventActions(eventId: EventId, count: Int) extends ShardMetadataAction

case class FindChildren(path: Path) extends ShardMetadataAction
case class FindSelectors(path: Path) extends ShardMetadataAction
case class FindDescriptors(path: Path, selector: JPath) extends ShardMetadataAction
case class FindPathMetadata(path: Path, selector: JPath) extends ShardMetadataAction

case class IngestBatchMetadata(metadata: Map[ProjectionDescriptor, ColumnMetadata], checkpoint: YggCheckpoint) extends ShardMetadataAction
case class FlushMetadata(serializationActor: ActorRef) extends ShardMetadataAction
