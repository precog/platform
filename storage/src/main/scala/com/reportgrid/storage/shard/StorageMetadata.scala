package com.reportgrid.storage.shard

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

import scala.collection.mutable.{Map => MMap, ListBuffer}

import scalaz._
import scalaz.Scalaz._

trait StorageMetadata {
  def find(path: Path, selector: JPath): Future[Map[ValueType, Set[Metadata]]]

  def findByType(path: Path, selector: JPath, valueType: ValueType): Future[Option[Set[Metadata]]] = 
    find(path, selector).map(_.get(valueType))

}

class ShardMetadata(actor: ActorRef) extends StorageMetadata {

  def checkpoints: Future[Map[Int, Int]] = {
    actor ? GetCheckpoints map { _.asInstanceOf[Map[Int, Int]] }
  }

  def update(pid: Int, eid: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadata: Seq[Set[Metadata]]): Future[Unit] = {
    actor ? UpdateMetadata(pid, eid, desc, values, metadata) map { _.asInstanceOf[Unit] } 
  }

  def find(path: Path, selector: JPath): Future[Map[ValueType, Set[Metadata]]] = {
    actor ? FindMetadata(path, selector) map { _.asInstanceOf[Map[ValueType, Set[Metadata]]] }
  }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } 

}

object ShardMetadata {
  def shardMetadata(filename: String) = dummyShardMetadata

  def dummyShardMetadata = {
    new ShardMetadata(dummyShardMetadataActor)
  }

  def dummyShardMetadataActor = actorOf(new ShardMetadataActor(dummyProjections, dummyCheckpoints)).start

  def dummyProjections = {
    MMap[ProjectionDescriptor, Seq[MMap[MetadataType, Metadata]]](
      ProjectionDescriptor(List(
        QualifiedSelector("/test/path/", JPath(".selector"), ValueType.Long),
        QualifiedSelector("/test/path/one", JPath(".selector"), ValueType.Long),
        QualifiedSelector("/test/path/", JPath(".notSelector"), ValueType.Long)), Set()) ->
        List[Set[Metadata]](Set(Ownership(Set("1", "2", "3"))), Set(Ownership(Set("d", "e", "f"))), Set(Ownership(Set("a", "b", "c")))).map( Metadata.toTypedMap _ ),
      ProjectionDescriptor(List(
        QualifiedSelector("/test/path/", JPath(".selector"), ValueType.Long),
        QualifiedSelector("/test/path/one", JPath(".selector"), ValueType.Long),
        QualifiedSelector("/test/path/", JPath(".notSelector"), ValueType.Long)), Set()) ->
        List[Set[Metadata]](Set(Ownership(Set("11", "22", "33"))), Set(Ownership(Set("dd", "ee", "ff"))), Set(Ownership(Set("aa", "bb", "cc")))).map( Metadata.toTypedMap _ ),
      ProjectionDescriptor(List(
        QualifiedSelector("/test/path/", JPath(".selector"), ValueType.String(None)),
        QualifiedSelector("/test/path/one", JPath(".selector"), ValueType.String(None)),
        QualifiedSelector("/test/path/", JPath(".notSelector"), ValueType.String(None))), Set()) ->
        List[Set[Metadata]](Set(Ownership(Set("01", "02", "03"))), Set(Ownership(Set("xd", "xe", "xf"))), Set(Ownership(Set("xa", "xb", "xc")))).map( Metadata.toTypedMap _ )
    )
  }
  
  def dummyCheckpoints = {
    MMap() += (1 -> 100) += (2 -> 101) += (3 -> 1000)
  }
}

class ShardMetadataActor(projections: MMap[ProjectionDescriptor, Seq[MMap[MetadataType, Metadata]]], checkpoints: MMap[Int, Int]) extends Actor {
 
  type MetadataMap = MMap[MetadataType, Metadata] 

  private val expectedEventActions = MMap[(Int, Int), Int]()
 
  def receive = {
   
    case ExpectedEventActions(producerId, eventId, expected) => 
      self.reply(setExpectation(producerId, eventId, expected))
    
    case UpdateMetadata(producerId, eventId, desc, values, metadata) => 
      self.reply(update(producerId, eventId, desc, values, metadata))
    
    case FindMetadata(path, selector)   => self.reply(find(path, selector))
    
    case GetCheckpoints                 => self.reply(checkpoints.toMap)
    
    case SyncMetadata                   => self.reply(sync)
  
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
      desc.columns.map { qs => MMap[MetadataType, Metadata]() + (ProjectionMetadata -> ProjectionMetadata(desc)) }
    }
    
    updateExpectation(producerId, eventId, recordCheckpoint _)

    projections.put(desc, applyMetadata(desc, values, metadata))
  }
  
  def find(path: Path, selector: JPath): Map[ValueType, Set[Metadata]] = {

    def matchingColumn(qs: QualifiedSelector) = qs.path == path && qs.selector == selector

    def matchingColumns(projs: MMap[ProjectionDescriptor, Seq[MetadataMap]]) =
      projs.toSeq flatMap { t =>
        t._1.columns zip t._2 filter { it => matchingColumn(it._1) }
      }
      
    def convertMetadata(columns: Seq[(QualifiedSelector, MetadataMap)]): Map[ValueType, MetadataMap] = {
      columns.foldLeft(Map[ValueType, MetadataMap]()) { (acc, t) => 
        acc + (t._1.valueType -> acc.get(t._1.valueType).map(_ |+| t._2).getOrElse(t._2))
      }
    }

    matchingColumns _ andThen convertMetadata _ apply projections mapValues { _.values.toSet }
  }  

  def sync = {

  }
}

sealed trait ShardMetadataAction

case object GetCheckpoints extends ShardMetadataAction
case class ExpectedEventActions(producerId: Int, eventId: Int, count: Int) extends ShardMetadataAction

case class UpdateMetadata(producerId: Int, eventId: Int, desc: ProjectionDescriptor, values: Seq[JValue], metadta: Seq[Set[Metadata]]) extends ShardMetadataAction
case class FindMetadata(path: Path, selector: JPath) extends ShardMetadataAction
case object SyncMetadata extends ShardMetadataAction

