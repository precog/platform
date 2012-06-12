package com.precog.yggdrasil
package actor 

import metadata._
import ColumnMetadata._

import com.precog.util._
import com.precog.common._

import com.weiglewilczek.slf4s.Logging

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import blueeyes.json.xschema.Decomposer
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.dispatch.{ExecutionContext, Future, MessageDispatcher}

import scalaz.{Failure, Success, Validation}
import scalaz.syntax.semigroup._
import scalaz.std.map._

object MetadataActor {
  // TODO: Where/how is the best place/way to isolate this?
  private[actor] class State(
      metadataStorage: MetadataStorage,
      private[MetadataActor] var messageClock: VectorClock, 
      private[MetadataActor] var kafkaOffset: Option[Long],
      private[MetadataActor] var projections: Map[ProjectionDescriptor, ColumnMetadata] = Map()) { metadataActor =>

    import ProjectionMetadata._

    private[MetadataActor] var dirty: Set[ProjectionDescriptor] = Set()

    def status: JValue = JObject(JField("Metadata", JObject(JField("state", JString("Ice cream!")) :: Nil)) :: Nil) // TODO: no, really...

    def saveMessage = SaveMetadata(fullDataFor(dirty), messageClock, kafkaOffset)
   
    // TODO: This feels like too much mixing between inter-related classes
    def fullDataFor(projs: Set[ProjectionDescriptor]): Map[ProjectionDescriptor, ColumnMetadata] = {
      projs.toList.map { descriptor => (descriptor -> columnMetadataFor(descriptor)) } toMap
    }

    private def columnMetadataFor(descriptor: ProjectionDescriptor) = 
      projections.get(descriptor).getOrElse {
        metadataStorage.currentMetadata(descriptor).unsafePerformIO match {
          case Success(record) => 
            projections += (descriptor -> record.metadata)
            record.metadata
          
          case Failure(errors) => sys.error("Failed to load metadata for " + descriptor)
        }
      }

    private def isChildPath(ref: Path, test: Path): Boolean = 
      test.elements.startsWith(ref.elements) && 
      test.elements.size > ref.elements.size
  
    def findChildren(path: Path): Set[Path] = 
      metadataStorage.flatMapDescriptors { descriptor => 
        descriptor.columns.collect { 
          case ColumnDescriptor(cpath, cselector, _, _) if isChildPath(path, cpath) => Path(cpath.elements(path.elements.length))
      }
    }.toSet
  
    def findSelectors(path: Path): Seq[JPath] = 
      metadataStorage.flatMapDescriptors { descriptor =>
        descriptor.columns.collect { 
          case ColumnDescriptor(cpath, cselector, _, _) if path == cpath => cselector 
        }
      }
  
    def findDescriptors(path: Path, selector: JPath): Map[ProjectionDescriptor, ColumnMetadata] = {
      @inline def isEqualOrChild(ref: JPath, test: JPath) = test.nodes startsWith ref.nodes
  
      @inline def matches(path: Path, selector: JPath) = (col: ColumnDescriptor) => {
        col.path == path && isEqualOrChild(selector, col.selector)
      }
  
      fullDataFor(metadataStorage.findDescriptors {
        descriptor => descriptor.columns.exists(matches(path, selector))
      })
    } 
  
  
    case class ResolvedSelector(selector: JPath, authorities: Authorities, descriptor: ProjectionDescriptor, metadata: ColumnMetadata) {
      def columnType: CType = descriptor.columns.find(_.selector == selector).map(_.valueType).get
    }
    
    @inline def isEqualOrChild(ref: JPath, test: JPath) = test.nodes startsWith ref.nodes

    @inline def matches(path: Path, selector: JPath) = (col: ColumnDescriptor) => {
      col.path == path && isEqualOrChild(selector, col.selector)
    }

    @inline def matching(path: Path, selector: JPath): Seq[ResolvedSelector] = 
      metadataStorage.flatMapDescriptors {
        descriptor => descriptor.columns.collect {
          case col @ ColumnDescriptor(_,sel, _,auth) if matches(path,selector)(col) => {
            ResolvedSelector(sel, auth, descriptor, columnMetadataFor(descriptor))
          }
        }
      }

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
  }
}

class MetadataActor(shardId: String, metadataStorage: MetadataStorage, checkpointCoordination: CheckpointCoordination) extends Actor { metadataActor =>
  private var state: MetadataActor.State = _

  override def preStart(): Unit = {
    val (messageClock, kafkaOffset) = checkpointCoordination.loadYggCheckpoint(shardId) match {
      case Some(Success(checkpoint)) =>
        (checkpoint.messageClock, Some(checkpoint.offset))

      case Some(Failure(errors)) =>
        sys.error("Unable to load Kafka checkpoint: " + errors)

      case None =>
        (VectorClock.empty, None)
    } 
    
    state = new MetadataActor.State(metadataStorage, messageClock, kafkaOffset)
  }

  def receive = {
    case Status => sender ! state.status

    case IngestBatchMetadata(patch, batchClock, batchOffset) => 
      println("Updating metadata")
      state.projections = state.projections |+| patch
      state.dirty = state.dirty ++ patch.keySet
      state.messageClock = state.messageClock |+| batchClock
      state.kafkaOffset = batchOffset orElse state.kafkaOffset
   
    case FindChildren(path)                   => sender ! state.findChildren(path)
    
    case FindSelectors(path)                  => sender ! state.findSelectors(path)

    case FindDescriptors(path, selector)      => sender ! state.findDescriptors(path, selector)

    case FindPathMetadata(path, selector)     => sender ! state.findPathMetadata(path, selector)

    case FindDescriptorRoot(descriptor)       => sender ! metadataStorage.findDescriptorRoot(descriptor)
    
    case FlushMetadata(serializationActor)    => serializationActor.tell(state.saveMessage, sender)
  }
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
case class FindDescriptorRoot(desc: ProjectionDescriptor) extends ShardMetadataAction

case class IngestBatchMetadata(metadata: Map[ProjectionDescriptor, ColumnMetadata], messageClock: VectorClock, kafkaOffset: Option[Long]) extends ShardMetadataAction
case class FlushMetadata(serializationActor: ActorRef) extends ShardMetadataAction

// Sort of a replacement for LocalMetadata that really uses the underlying machinery
class TestMetadataActorish(initial: Map[ProjectionDescriptor,ColumnMetadata], storage: MetadataStorage)(implicit val dispatcher: MessageDispatcher, context: ExecutionContext) extends StorageMetadata {
  private val state = new MetadataActor.State(storage, VectorClock.empty, None, initial)

  def findChildren(path: Path): Future[Set[Path]] = Future(state.findChildren(path))
  def findSelectors(path: Path): Future[Seq[JPath]] = Future(state.findSelectors(path))
  def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]] = Future(state.findDescriptors(path, selector))
  def findPathMetadata(path: Path, selector: JPath): Future[PathRoot] = Future(state.findPathMetadata(path, selector))

}
