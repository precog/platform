package com.reportgrid.yggdrasil
package shard

import com.reportgrid.common._
import com.reportgrid.analytics.Path

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor._
import akka.actor.Actor._
import akka.routing._
import akka.dispatch.Future
import akka.util.Timeout
import akka.util.duration._

import scala.collection._
import scala.collection.immutable.ListMap
import scala.collection.immutable.Set

import scalaz._
import scalaz.Scalaz._

trait StorageMetadata {
  def find(path: Path, selector: JPath): Future[Map[SType, Set[Metadata]]]

  def findByType(path: Path, selector: JPath, valueType: SType): Future[Option[Set[Metadata]]] = 
    find(path, selector).map(_.get(valueType))

}

class ShardMetadata(actor: ActorRef) extends StorageMetadata {

  implicit val serviceTimeout: Timeout = 2 seconds 
  
  def find(path: Path, selector: JPath): Future[Map[SType, Set[Metadata]]] = {
    actor ? FindMetadata(path, selector) map { _.asInstanceOf[Map[SType, Set[Metadata]]] }
  }

  def checkpoints: Future[Map[Int, Int]] = {
    actor ? GetCheckpoints map { _.asInstanceOf[Map[Int, Int]] }
  }

  def update(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]]): Future[Unit] = {
    actor ? UpdateMetadata(pid, eid, desc, values, metadata) map { _.asInstanceOf[Unit] } 
  }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } 

}


object ShardMetadata {
  def shardMetadata(filename: String) = dummyShardMetadata

  def dummyShardMetadata = {
    new ShardMetadata(dummyShardMetadataActor)
  }
  
  def actorSystem = ActorSystem("test actor system")

  def dummyShardMetadataActor = actorSystem.actorOf(Props(new ShardMetadataActor(dummyProjections, dummyCheckpoints)))

  def dummyProjections = {
    mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]](
      projectionHelper(List(
        ColumnDescriptor(Path("/test/path/"), JPath(".selector"), SLong),
        ColumnDescriptor(Path("/test/path/one"), JPath(".selector"), SLong),
        ColumnDescriptor(Path("/test/path/"), JPath(".notSelector"), SLong))) -> List(mutable.Map(),mutable.Map(),mutable.Map()),
      projectionHelper(List(
        ColumnDescriptor(Path("/test/path/"), JPath(".selector"), SLong),
        ColumnDescriptor(Path("/test/path/one"), JPath(".selector"), SLong),
        ColumnDescriptor(Path("/test/path/"), JPath(".notSelector"), SLong))) -> List(mutable.Map(),mutable.Map(),mutable.Map()),
      projectionHelper(List(
        ColumnDescriptor(Path("/test/path/"), JPath(".selector"), SLong),
        ColumnDescriptor(Path("/test/path/one"), JPath(".selector"), SLong),
        ColumnDescriptor(Path("/test/path/"), JPath(".notSelector"), SLong))) -> List(mutable.Map(),mutable.Map(),mutable.Map()))
  }
 
  def projectionHelper(cds: Seq[ColumnDescriptor]): ProjectionDescriptor = {
    val columns = cds.foldLeft(ListMap[ColumnDescriptor, Int]()) { (acc, el) =>
      acc + (el -> 0)
    }
    val sort = List( (cds(0), ById) ) 
    ProjectionDescriptor(columns, sort) match {
      case Success(pd) => pd
      case _           => sys.error("Bang, bang on the door")
    }
  }

  def dummyCheckpoints = {
    mutable.Map() += (1 -> 100) += (2 -> 101) += (3 -> 1000)
  }
}


class ShardMetadataActor(projections: mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]], checkpoints: mutable.Map[Int, Int]) extends Actor {
 
  type MetadataMap = mutable.Map[MetadataType, Metadata] 

  private val expectedEventActions = mutable.Map[(Int, Int), Int]()
 
  def receive = {
   
    case ExpectedEventActions(producerId, eventId, expected) => 
      sender ! setExpectation(producerId, eventId, expected)
    
    case UpdateMetadata(producerId, eventId, desc, values, metadata) => 
      sender ! update(producerId, eventId, desc, values, metadata)
    
    case FindMetadata(path, selector)   => sender ! find(path, selector)
    
    case GetCheckpoints                 => sender ! checkpoints.toMap
    
    case SyncMetadata                   => sender ! sync
  
  }

  def setExpectation(producerId: Int, eventId: Int, expected: Int) = {
    expectedEventActions.put((producerId, eventId), expected)
  }
 
  def update(producerId: Int, eventId: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]]): Unit = {
    def updateExpectation(producerId: Int, eventId: Int, ifSatisfied: (Int, Int) => Unit) = {
      decrementExpectation(producerId, eventId) match {
        case Some(0) => ifSatisfied(producerId, eventId)
        case _       => ()
      }
    }
    
    def decrementExpectation(producerId: Int, eventId: Int): Option[Int] = {
      val key = (producerId, eventId)
      val update = expectedEventActions.get(key).map( _ - 1 )
      update.foreach {
        case x if x > 0  => expectedEventActions.put(key, x)
        case x if x <= 0 => expectedEventActions.remove(key)
      }
      update
    }
    
   
    def recordCheckpoint(producerId: Int, eventId: Int) {
      val newVal = checkpoints.get(producerId).map{ cur => if(cur < eventId) eventId else cur }.getOrElse(eventId)
      checkpoints.put(producerId, newVal)
    }

    def applyMetadata(desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]]): Seq[MetadataMap] = {
      addValueMetadata(values) _ andThen combineMetadata(metadata) _ apply projections.get(desc).getOrElse(initMetadata(desc))
    }

    def addValueMetadata(values: Seq[JValue])(metadata: Seq[MetadataMap]): Seq[MetadataMap] = {
      values zip metadata map { t => Metadata.typedValueStats(t._1).map( t._2 + _ ).getOrElse(t._2) }
    }

    def combineMetadata(user: Seq[Set[Metadata]])(metadata: Seq[MetadataMap]): Seq[MetadataMap] = {
      user zip metadata map { t => Metadata.toTypedMap(t._1) |+| t._2 }
    }

    def initMetadata(desc: ProjectionDescriptor): Seq[MetadataMap] = {
      desc.columns map { cd => mutable.Map[MetadataType, Metadata]() } toSeq
    }
    
    updateExpectation(producerId, eventId, recordCheckpoint _)

    projections.put(desc, applyMetadata(desc, values, metadata))
  }
  
  def find(path: Path, selector: JPath): Map[SType, Set[Metadata]] = {

    def matchingColumn(cd: ColumnDescriptor) = cd.path == path && cd.selector == selector

    def matchingColumns(projs: mutable.Map[ProjectionDescriptor, Seq[MetadataMap]])  =
      projs.toSeq flatMap { 
        case (descriptor, metadata) =>
          descriptor.columns zip metadata filter { 
            case (colDesc, _) => matchingColumn(colDesc)
          }
      }
      
    def convertMetadata(columns: Seq[(ColumnDescriptor, MetadataMap)]): Map[ColumnType, MetadataMap] = {
      columns.foldLeft(Map.empty[ColumnType, MetadataMap]) { 
        case (acc, (colDesc, mmap)) => 
          acc + (colDesc.valueType -> acc.get(colDesc.valueType).map(_ |+| mmap).getOrElse(mmap))
      }
    }

    //matchingColumns _ andThen convertMetadata _ apply projections mapValues { _.values.toSet }
    sys.error("todo")
  }  

  def sync = {

  }
}

sealed trait ShardMetadataAction

case object GetCheckpoints extends ShardMetadataAction
case class ExpectedEventActions(producerId: Int, eventId: Int, count: Int) extends ShardMetadataAction

case class UpdateMetadata(producerId: Int, eventId: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]]) extends ShardMetadataAction
case class FindMetadata(path: Path, selector: JPath) extends ShardMetadataAction
case object SyncMetadata extends ShardMetadataAction

