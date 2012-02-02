package com.precog.yggdrasil
package shard

import com.precog.common._
import com.precog.analytics.Path

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor.{IO => _, _}
import akka.actor.Actor._
import akka.pattern.ask
import akka.routing._
import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Timeout
import akka.util.duration._

import scala.collection._
import scala.collection.immutable.ListMap
import scala.collection.immutable.Set

import scalaz._
import scalaz.syntax
import scalaz.effect._
import scalaz.Scalaz._

object StorageMetadata {
  type ColumnMetadata = Map[ColumnDescriptor, Map[MetadataType, Metadata]]
}

trait StorageMetadata {
  import StorageMetadata._

  implicit val dispatcher: MessageDispatcher

  def findSelectors(path: Path): Future[Seq[JPath]]

  def findProjections(path: Path): Future[Map[ProjectionDescriptor, ColumnMetadata]] = {
    findSelectors(path).flatMap { selectors => 
      Future.traverse( selectors )( findProjections(path, _) ) map { _.reduce(_ ++ _) }
    }
  }

  def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]]

  def findProjections(path: Path, selector: JPath, valueType: SType): Future[Map[ProjectionDescriptor, ColumnMetadata]] = 
    findProjections(path, selector) map { m => m.filter(typeFilter(path, selector, valueType) _ ) }

  def typeFilter(path: Path, selector: JPath, valueType: SType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean = {
    t._1.columns.exists( col => col.path == path && col.selector == selector && col.valueType == valueType )
  }
}

class ShardMetadata(actor: ActorRef, messageDispatcher: MessageDispatcher) extends StorageMetadata {
  import StorageMetadata._

  implicit val dispatcher = messageDispatcher

  implicit val serviceTimeout: Timeout = 10 seconds
 
  def findSelectors(path: Path) = actor ? FindSelectors(path) map { _.asInstanceOf[Seq[JPath]] }

  def findProjections(path: Path, selector: JPath) = 
    actor ? FindDescriptors(path, selector) map { _.asInstanceOf[Map[ProjectionDescriptor, ColumnMetadata]] }

  def checkpoints: Future[Map[Int, Int]] = actor ? GetCheckpoints map { _.asInstanceOf[Map[Int, Int]] }

  def update(eventId: EventId, desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]): Future[Unit] = {
    actor ? UpdateMetadata(eventId, desc, values, metadata) map { _.asInstanceOf[Unit] } 
  }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } 

}

class ShardMetadataActor(projections: mutable.Map[ProjectionDescriptor, Seq[MetadataMap]], checkpoints: mutable.Map[Int, Int]) extends Actor {

  private val expectedEventActions = mutable.Map[EventId, Int]()
 
  def receive = {
   
    case ExpectedEventActions(eventId, expected) => 
      sender ! setExpectation(eventId, expected)
    
    case UpdateMetadata(eventId, desc, values, metadata) => 
      sender ! update(eventId, desc, values, metadata)
   
    case FindSelectors(path)                  => sender ! findSelectors(path)

    case FindDescriptors(path, selector)      => sender ! findDescriptors(path, selector)
    
    case GetCheckpoints                       => sender ! checkpoints.toMap
    
    case FlushMetadata(serializationActor)    => sender ! (serializationActor ! SaveMetadata(projections.clone))
    
    case FlushCheckpoints(serializationActor) => sender ! (serializationActor ! SaveCheckpoints(checkpoints.clone)) 
  
  }

  def setExpectation(eventId: EventId, expected: Int) = {
    expectedEventActions.put(eventId, expected)
  }

  def update(eventId: EventId, desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]): Unit = {
    import MetadataUpdateHelper._ 
   
    updateExpectation(eventId, recordCheckpoint(checkpoints) _, expectedEventActions)

    projections.put(desc, applyMetadata(desc, values, metadata, projections))
  }
 
  def findSelectors(path: Path): Seq[JPath] = {
    projections.toSeq flatMap {
      case (descriptor, _) => descriptor.columns.collect { case ColumnDescriptor(cpath, cselector, _, _) if path == cpath => cselector }
    }
  }

  def findDescriptors(path: Path, selector: JPath): Map[ProjectionDescriptor, Map[ColumnDescriptor, Map[MetadataType, Metadata]]] = {
    
    def matches(path: Path, selector: JPath)(col: ColumnDescriptor) = col.path == path && col.selector == selector

    projections.filter {
      case (descriptor, _) => descriptor.columns.exists(matches(path, selector))
      case _               => false
    }.map {
      case (descriptor, metadata) => (descriptor, Map( (descriptor.columns zip metadata): _*))
    }
  }  
}

object MetadataUpdateHelper {
 def updateExpectation(eventId: EventId, ifSatisfied: (EventId) => Unit, expectedEventActions: mutable.Map[EventId, Int]) = {
    decrementExpectation(eventId, expectedEventActions) match {
      case Some(0) => ifSatisfied(eventId)
      case _       => ()
    }
  }
  
  def decrementExpectation(eventId: EventId, expectedEventActions: mutable.Map[EventId, Int]): Option[Int] = {
    val update = expectedEventActions.get(eventId).map( _ - 1 )
    update.foreach {
      case x if x > 0  => expectedEventActions.put(eventId, x)
      case x if x <= 0 => expectedEventActions.remove(eventId)
    }
    update
  }

  def recordCheckpoint(checkpoints: mutable.Map[Int, Int])(eventId: EventId) {
    eventId match {
      case EventId(pid, sid) => {
        val newVal = checkpoints.get(pid).map{ cur => if(cur < sid) sid else cur }.getOrElse(sid)
        checkpoints.put(pid, newVal)
      }
    }
  }

  def applyMetadata(desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]], projections: mutable.Map[ProjectionDescriptor, Seq[MetadataMap]]): Seq[MetadataMap] = {
    combineMetadata(projections.get(desc).getOrElse(initMetadata(desc)))(addValueMetadata(values)(metadata map { Metadata.toTypedMap }))
  }

  def addValueMetadata(values: Seq[CValue])(metadata: Seq[MetadataMap]): Seq[MetadataMap] = {
    values zip metadata map { t => valueStats(t._1).map( vs => t._2 + (vs.metadataType -> vs) ).getOrElse(t._2) }
  }

  def combineMetadata(user: Seq[MetadataMap])(metadata: Seq[MetadataMap]): Seq[MetadataMap] = {
    user zip metadata map { t => t._1 |+| t._2 }
  }

  def initMetadata(desc: ProjectionDescriptor): Seq[MetadataMap] = {
    desc.columns map { cd => mutable.Map[MetadataType, Metadata]() } toSeq
  }

 def valueStats(cval: CValue): Option[Metadata] = cval match {
//   case JBool(b)     => Some(BooleanValueStats(1, if(b) 1 else 0))
//   case JInt(i)      => Some(BigDecimalValueStats(1, BigDecimal(i), BigDecimal(i)))
//   case JDouble(d)   => Some(DoubleValueStats(1, d, d))
//   case JString(s)   => Some(StringValueStats(1, s, s))
   case _            => sys.error("todo") 
 }
}

sealed trait ShardMetadataAction

case class ExpectedEventActions(eventId: EventId, count: Int) extends ShardMetadataAction

case class FindSelectors(path: Path) extends ShardMetadataAction
case class FindDescriptors(path: Path, selector: JPath) extends ShardMetadataAction

case class UpdateMetadata(eventId: EventId, desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]) extends ShardMetadataAction
case class FlushMetadata(serializationActor: ActorRef) extends ShardMetadataAction
case class FlushCheckpoints(serializationActor: ActorRef) extends ShardMetadataAction

case object GetCheckpoints extends ShardMetadataAction

class MetadataSerializationActor(metadataIO: MetadataIO, checkpointIO: CheckpointIO) extends Actor {
  def receive = {
    case SaveMetadata(metadata) =>  sender ! metadata.toList.map {
        case (pd, md) => metadataIO(pd, md)
      }.sequence[IO, Unit].map(_ => ()).unsafePerformIO

    case SaveCheckpoints(checkpoints) => sender ! checkpointIO(checkpoints).unsafePerformIO
  }
}

sealed trait MetadataSerializationAction

case class SaveMetadata(metadata: mutable.Map[ProjectionDescriptor, Seq[MetadataMap]]) extends MetadataSerializationAction
case class SaveCheckpoints(metadata: mutable.Map[Int, Int]) extends MetadataSerializationAction
