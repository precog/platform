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

trait StorageMetadata {

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

  implicit val serviceTimeout: Timeout = 10 seconds
 
  def findSelectors(path: Path) = actor ? FindSelectors(path) map { _.asInstanceOf[Seq[JPath]] }

  def findProjections(path: Path, selector: JPath) = 
    actor ? FindDescriptors(path, selector) map { _.asInstanceOf[Map[ProjectionDescriptor, ColumnMetadata]] }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } 

}

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

  def update(inserts: List[InsertComplete]): Unit = {
    import MetadataUpdateHelper._ 
   
    projections = inserts.foldLeft(projections){ (acc, insert) =>
      acc + (insert.descriptor -> applyMetadata(insert.descriptor, insert.values, insert.metadata, acc))
    }
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

case class UpdateMetadata(inserts: List[InsertComplete]) extends ShardMetadataAction
case class FlushMetadata(serializationActor: ActorRef) extends ShardMetadataAction

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

case class SaveMetadata(metadata: Map[ProjectionDescriptor, ColumnMetadata], messageClock: VectorClock) extends MetadataSerializationAction
