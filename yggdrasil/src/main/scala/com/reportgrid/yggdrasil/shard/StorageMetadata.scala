package com.reportgrid.yggdrasil
package shard

import com.reportgrid.common._
import com.reportgrid.analytics.Path

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor.{IO => _, _}
import akka.actor.Actor._
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


trait StorageMetadata {
 
  type ColumnMetadata = Map[ColumnDescriptor, Map[MetadataType, Metadata]]

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

  implicit val dispatcher = messageDispatcher

  implicit val serviceTimeout: Timeout = 2 seconds
 
  def findSelectors(path: Path) = Future[Seq[JPath]] { List() }

  def findProjections(path: Path, selector: JPath) = Future[Map[ProjectionDescriptor, ColumnMetadata]] { Map() }

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
    new ShardMetadata(dummyShardMetadataActor, actorSystem.dispatcher)
  }
  
  def actorSystem = ActorSystem("test actor system")

  def echoMetadataIO(descriptor: ProjectionDescriptor, metadata: Seq[MetadataMap]) = IO {
    println("Saving metadata entry")
    println(descriptor)
    println(metadata)
  }

  def echoCheckpointIO(checkpoints: Checkpoints) = IO {
    println("Saving checkpoints")
    println(checkpoints) 
  }

  def dummyShardMetadataActor = actorSystem.actorOf(Props(new ShardMetadataActor(dummyProjections, dummyCheckpoints, echoMetadataIO _, echoCheckpointIO _)))

  def dummyProjections = {
    mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]](
      projectionHelper(List(
        ColumnDescriptor(Path("/test/path/"), JPath(".selector"), SLong, Ownership(Set())),
        ColumnDescriptor(Path("/test/path/one"), JPath(".selector"), SLong, Ownership(Set())),
        ColumnDescriptor(Path("/test/path/"), JPath(".notSelector"), SLong, Ownership(Set())))) -> List(mutable.Map(),mutable.Map(),mutable.Map()),
      projectionHelper(List(
        ColumnDescriptor(Path("/test/path/"), JPath(".selector"), SLong, Ownership(Set())),
        ColumnDescriptor(Path("/test/path/one"), JPath(".selector"), SLong, Ownership(Set())),
        ColumnDescriptor(Path("/test/path/"), JPath(".notSelector"), SLong, Ownership(Set())))) -> List(mutable.Map(),mutable.Map(),mutable.Map()),
      projectionHelper(List(
        ColumnDescriptor(Path("/test/path/"), JPath(".selector"), SLong, Ownership(Set())),
        ColumnDescriptor(Path("/test/path/one"), JPath(".selector"), SLong, Ownership(Set())),
        ColumnDescriptor(Path("/test/path/"), JPath(".notSelector"), SLong, Ownership(Set())))) -> List(mutable.Map(),mutable.Map(),mutable.Map()))
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

class ShardMetadataActor(projections: mutable.Map[ProjectionDescriptor, Seq[MetadataMap]], checkpoints: mutable.Map[Int, Int], metadataIO: MetadataIO, checkpointIO: CheckpointIO) extends Actor {

  private val expectedEventActions = mutable.Map[(Int, Int), Int]()
 
  def receive = {
   
    case ExpectedEventActions(producerId, eventId, expected) => 
      sender ! setExpectation(producerId, eventId, expected)
    
    case UpdateMetadata(producerId, eventId, desc, values, metadata) => 
      sender ! update(producerId, eventId, desc, values, metadata)
   
    case FindSelectors(path)             => sender ! findSelectors(path)

    case FindDescriptors(path, selector) => sender ! findDescriptors(path, selector)
    
    case GetCheckpoints                  => sender ! checkpoints.toMap
    
    case SyncMetadata                    => sender ! sync
  
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

  def sync = {
    metadataIO(projections) flatMap { _ => checkpointIO(checkpoints) } unsafePerformIO
  }

  def metadataIO(projections: Map[ProjectionDescriptor, Seq[MetadataMap]]): IO[Unit] =
    projections.map{ t => metadataIO(t._1, t._2) }.toList.sequence[IO, Unit].map{ _ => Unit}
}

sealed trait ShardMetadataAction

case class ExpectedEventActions(producerId: Int, eventId: Int, count: Int) extends ShardMetadataAction

case class FindSelectors(path: Path) extends ShardMetadataAction
case class FindDescriptors(path: Path, selector: JPath) extends ShardMetadataAction

case class UpdateMetadata(producerId: Int, eventId: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]]) extends ShardMetadataAction
case object SyncMetadata extends ShardMetadataAction

case object GetCheckpoints extends ShardMetadataAction


