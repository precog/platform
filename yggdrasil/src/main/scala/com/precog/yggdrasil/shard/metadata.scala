package com.precog.yggdrasil
package shard

import kafka._

import com.precog.common._
import com.precog.common.util._
import com.precog.common.security._
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

import scala.collection.mutable
import scala.collection.immutable.ListMap
import scala.collection.immutable.Set

import scalaz._
import scalaz.syntax
import scalaz.effect._
import scalaz.Scalaz._

import com.weiglewilczek.slf4s.Logging

object StorageMetadata {
  type ColumnMetadata = Map[ColumnDescriptor, Map[MetadataType, Metadata]]
  object ColumnMetadata {
    val Empty = Map.empty[ColumnDescriptor, Map[MetadataType, Metadata]]
  }
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
    t._1.columns.exists( col => col.path == path && col.selector == selector && col.valueType =~ valueType )
  }
}

trait MetadataView extends StorageMetadata

class IdentityMetadataView(metadata: StorageMetadata)(implicit val dispatcher: MessageDispatcher) extends MetadataView {
  def findSelectors(path: Path) = metadata.findSelectors(path)
  def findProjections(path: Path, selector: JPath) = metadata.findProjections(path, selector)
}

class UserMetadataView(uid: String, accessControl: AccessControl, metadata: StorageMetadata)(implicit val dispatcher: MessageDispatcher) extends MetadataView { 
  
  def findSelectors(path: Path) = {
    metadata.findSelectors(path) flatMap { selectors =>
      Future.traverse(selectors) { selector =>
        findProjections(path, selector) map { result =>
          if(result.isEmpty) List.empty else List(selector)
        }
      } map { _.flatten }
    }
  }

  def findProjections(path: Path, selector: JPath) = {
    metadata.findProjections(path, selector) map { _.filter {
        case (key, value) =>
          value forall { 
            case (colDesc, _) => 
              val uids = colDesc.authorities.uids
              accessControl.mayAccessData(uid, path, uids, DataQuery)
          }
      }
    }
  }
}

class ShardMetadata(actor: ActorRef)(implicit val dispatcher: MessageDispatcher) extends StorageMetadata {
  import StorageMetadata._

  implicit val serviceTimeout: Timeout = 10 seconds
 
  def findSelectors(path: Path) = actor ? FindSelectors(path) map { _.asInstanceOf[Seq[JPath]] }

  def findProjections(path: Path, selector: JPath) = 
    actor ? FindDescriptors(path, selector) map { _.asInstanceOf[Map[ProjectionDescriptor, ColumnMetadata]] }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } 

//  def checkpoints: Future[Map[Int, Int]] = actor ? GetCheckpoints map { _.asInstanceOf[Map[Int, Int]] }

}

class ShardMetadataActor(projections: mutable.Map[ProjectionDescriptor, Seq[MetadataMap]], initialClock: VectorClock) extends Actor {

  private var messageClock = initialClock 

  def receive = {
   
    case UpdateMetadata(inserts) => 
      sender ! update(inserts)
   
    case FindSelectors(path)                  => sender ! findSelectors(path)

    case FindDescriptors(path, selector)      => sender ! findDescriptors(path, selector)

    case FlushMetadata(serializationActor)    => serializationActor ! SaveMetadata(projections.clone, messageClock)
    
  }

  def update(inserts: List[InsertComplete]): Unit = {
    import MetadataUpdateHelper._ 
   
    inserts foreach { insert =>
      projections.put(insert.descriptor, applyMetadata(insert.descriptor, insert.values, insert.metadata, projections))
    }
  }
 
  def findSelectors(path: Path): Seq[JPath] = {
    projections.toSeq flatMap {
      case (descriptor, _) => descriptor.columns.collect { case ColumnDescriptor(cpath, cselector, _, _) if path == cpath => cselector }
    }
  }

  def findDescriptors(path: Path, selector: JPath): Map[ProjectionDescriptor, Map[ColumnDescriptor, Map[MetadataType, Metadata]]] = {
    @inline def matches(path: Path, selector: JPath) = (col: ColumnDescriptor) => col.path == path && col.selector == selector

    for ((descriptor, metadata) <- projections.toMap if descriptor.columns.exists(matches(path, selector))) 
    yield (descriptor, (descriptor.columns zip metadata.map(_.toMap)).toMap)
  }  
}

object MetadataUpdateHelper {
// def updateExpectation(eventId: EventId, ifSatisfied: (EventId) => Unit, expectedEventActions: mutable.Map[EventId, Int]) = {
//    decrementExpectation(eventId, expectedEventActions) match {
//      case Some(0) => ifSatisfied(eventId)
//      case _       => ()
//    }
//  }
//  
//  def decrementExpectation(eventId: EventId, expectedEventActions: mutable.Map[EventId, Int]): Option[Int] = {
//    val update = expectedEventActions.get(eventId).map( _ - 1 )
//    update.foreach {
//      case x if x > 0  => expectedEventActions.put(eventId, x)
//      case x if x <= 0 => expectedEventActions.remove(eventId)
//    }
//    update
//  }
//
//  def recordCheckpoint(checkpoints: mutable.Map[Int, Int])(eventId: EventId) {
//    eventId match {
//      case EventId(pid, sid) => {
//        val newVal = checkpoints.get(pid).map{ cur => if(cur < sid) sid else cur }.getOrElse(sid)
//        checkpoints.put(pid, newVal)
//      }
//    }
//  }

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

case class UpdateMetadata(inserts: List[InsertComplete]) extends ShardMetadataAction
case class FlushMetadata(serializationActor: ActorRef) extends ShardMetadataAction
case class FlushCheckpoints(serializationActor: ActorRef) extends ShardMetadataAction

case object GetCheckpoints extends ShardMetadataAction

class MetadataSerializationActor(checkpoints: YggCheckpoints, metadataIO: MetadataIO) extends Actor with Logging {
  def receive = {
    case SaveMetadata(metadata, messageClock) => 
      logger.debug("Syncing metadata")
      metadata.toList.map {
        case (pd, md) => metadataIO(pd, md)
      }.sequence[IO, Unit].map(_ => ()).unsafePerformIO
      checkpoints.metadataPersisted(messageClock)
  }
}

sealed trait MetadataSerializationAction

case class SaveMetadata(metadata: mutable.Map[ProjectionDescriptor, Seq[MetadataMap]], messageClock: VectorClock) extends MetadataSerializationAction
case class SaveCheckpoints(metadata: mutable.Map[Int, Int]) extends MetadataSerializationAction
