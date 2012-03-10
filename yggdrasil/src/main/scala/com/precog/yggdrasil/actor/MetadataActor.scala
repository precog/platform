package com.precog.yggdrasil
package actor 

import metadata._

import com.precog.common._
import com.precog.common.util._

import blueeyes.json.JPath

import akka.actor.Actor
import akka.actor.ActorRef

import scalaz.Scalaz._

class ShardMetadataActor(initialProjections: Map[ProjectionDescriptor, ColumnMetadata], initialClock: VectorClock) extends Actor {

  private var projections = initialProjections

  private var messageClock = initialClock 

  def receive = {
   
    case UpdateMetadata(inserts) => 
      sender ! update(inserts)
   
    case FindSelectors(path)                  => sender ! findSelectors(path)

    case FindDescriptors(path, selector)      => sender ! findDescriptors(path, selector)

    case FlushMetadata(serializationActor)    => sender ! (serializationActor ! SaveMetadata(projections, messageClock))
    
  }

  def update(inserts: Seq[InsertComplete]): Unit = {
    import MetadataUpdateHelper._ 
   
    val (projUpdate, clockUpdate) = inserts.foldLeft(projections, messageClock){ 
      case ((projs, clock), insert) =>
        (projs + (insert.descriptor -> applyMetadata(insert.descriptor, insert.values, insert.metadata, projs)),
         clock.update(insert.eventId.producerId, insert.eventId.sequenceId))
    }

    projections = projUpdate
    messageClock = clockUpdate
  }
 
  def findSelectors(path: Path): Seq[JPath] = {
    projections.foldLeft(Vector[JPath]()) {
      case (acc, (descriptor, _)) => acc ++ descriptor.columns.collect { case ColumnDescriptor(cpath, cselector, _, _) if path == cpath => cselector }
    }
  }

  def findDescriptors(path: Path, selector: JPath): Map[ProjectionDescriptor, ColumnMetadata] = {
    @inline def isEqualOrChild(ref: JPath, test: JPath) = test.nodes startsWith ref.nodes

    @inline def matches(path: Path, selector: JPath) = (col: ColumnDescriptor) => {
      col.path == path && isEqualOrChild(selector, col.selector)
    }

    projections.filter {
      case (descriptor, _) => descriptor.columns.exists(matches(path, selector))
    }
  }  
}

object MetadataUpdateHelper {

  def applyMetadata(desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]], projections: Map[ProjectionDescriptor, ColumnMetadata]): ColumnMetadata = {
    val initialMetadata = projections.get(desc).getOrElse(initMetadata(desc))
    val userAndValueMetadata = addValueMetadata(values, metadata.map { Metadata.toTypedMap _ })

    combineMetadata(desc, initialMetadata, userAndValueMetadata)
  }

  def addValueMetadata(values: Seq[CValue], metadata: Seq[MetadataMap]): Seq[MetadataMap] = {
    values zip metadata map { t => valueStats(t._1).map( vs => t._2 + (vs.metadataType -> vs) ).getOrElse(t._2) }
  }

  def combineMetadata(desc: ProjectionDescriptor, existingMetadata: ColumnMetadata, newMetadata: Seq[MetadataMap]): ColumnMetadata = {
    val newColumnMetadata = desc.columns zip newMetadata
    newColumnMetadata.foldLeft(existingMetadata) { 
      case (acc, (col, newColMetadata)) =>
        val updatedMetadata = acc.get(col) map { _ |+| newColMetadata } getOrElse { newColMetadata }
        acc + (col -> updatedMetadata)
    }
  }

  def initMetadata(desc: ProjectionDescriptor): ColumnMetadata = 
    desc.columns.foldLeft( Map[ColumnDescriptor, MetadataMap]() ) {
      (acc, col) => acc + (col -> Map[MetadataType, Metadata]())
    }

 def valueStats(cval: CValue): Option[Metadata] = cval.fold( 
   str = (s: String)      => Some(StringValueStats(1, s, s)),
   bool = (b: Boolean)    => Some(BooleanValueStats(1, if(b) 1 else 0)),
   int = (i: Int)         => Some(LongValueStats(1, i, i)),
   long = (l: Long)       => Some(LongValueStats(1, l, l)),
   float = (f: Float)     => Some(DoubleValueStats(1, f, f)),
   double = (d: Double)   => Some(DoubleValueStats(1, d, d)),
   num = (bd: BigDecimal) => Some(BigDecimalValueStats(1, bd, bd)),
   emptyObj = None,
   emptyArr = None,
   nul = None)
 
}   

sealed trait ShardMetadataAction

case class ExpectedEventActions(eventId: EventId, count: Int) extends ShardMetadataAction

case class FindSelectors(path: Path) extends ShardMetadataAction
case class FindDescriptors(path: Path, selector: JPath) extends ShardMetadataAction

case class UpdateMetadata(inserts: Seq[InsertComplete]) extends ShardMetadataAction
case class FlushMetadata(serializationActor: ActorRef) extends ShardMetadataAction
