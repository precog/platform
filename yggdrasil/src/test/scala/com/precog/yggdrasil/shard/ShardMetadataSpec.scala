package com.precog.yggdrasil.shard

import org.specs2.mutable.Specification

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import org.scalacheck.Gen._

import com.precog.analytics.Path

import com.precog.yggdrasil._

import com.precog.common._
import com.precog.common.Event
import com.precog.common.util.RealisticIngestMessage

import scala.collection.mutable
import scala.collection.immutable.ListMap

import akka.actor._
import akka.pattern.ask
import akka.util._
import akka.util.duration._
import akka.dispatch._

class ShardMetadataSpec extends Specification with RealisticIngestMessage {
  val sample = containerOfN[List, Event](50, genEvent).sample
 
  def buildMetadata(sample: List[Event]): mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]] = {
    def projectionDescriptor(e: Event): Set[ProjectionDescriptor] = { e match {
      case Event(path, tokenId, data, _) => data.flattenWithPath.map {
        case (sel, value) => ColumnDescriptor(path, sel, typeOf(value), Ownership(Set(tokenId)))
      }
    } }.map{ cd => ProjectionDescriptor( ListMap() + (cd -> 0), List[(ColumnDescriptor, SortBy)]() :+ (cd, ById)).toOption.get }.toSet

    def typeOf(jvalue: JValue): ColumnType = {
      ColumnType.forValue(jvalue).getOrElse(SNull)
    }

    def columnMetadata(columns: Seq[ColumnDescriptor]): Seq[mutable.Map[MetadataType, Metadata]] = 
      columns.map{ _ => mutable.Map[MetadataType, Metadata]() }

    sample.map(projectionDescriptor).foldLeft( mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]]()) {
      case (acc, el) => el.foldLeft(acc) {
        case (iacc, pd) => iacc + (pd -> columnMetadata(pd.columns)) 
      }
    }
  }
 
  "ShardMetadata" should {
 
    implicit val timeout = Timeout(Long.MaxValue)
   
    def extractSelectorsFor(path: Path)(events: List[Event]): Set[JPath] = {
      events.flatMap {
        case Event(epath, token, data, metadata) if epath == path => data.flattenWithPath.map(_._1) 
        case _                                                    => List.empty
      }.toSet
    }

    def extractMetadataFor(path: Path, selector: JPath)(events: List[Event]): mutable.Map[ProjectionDescriptor, mutable.Map[ColumnDescriptor, mutable.Map[MetadataType, Metadata]]] = {
      def convertColDesc(cd: ColumnDescriptor) = mutable.Map[ColumnDescriptor, mutable.Map[MetadataType, Metadata]]() + (cd -> mutable.Map[MetadataType, Metadata]())
      mutable.Map(events.flatMap {
        case e @ Event(epath, token, data, metadata) if epath == path && data.flattenWithPath.exists(_._1 == selector) => List(toProjectionDescriptor(e, selector))
        case _                                                                              => List.empty
      }.map{ pd => (pd, convertColDesc(pd.columns.head)) }: _*)
    }

    def toProjectionDescriptor(e: Event, selector: JPath) = {
      def extractType(selector: JPath, data: JValue): ColumnType = {
        data.flattenWithPath.find( _._1 == selector).flatMap[ColumnType]( t => ColumnType.forValue(t._2) ).getOrElse(SNull)
      }
      val colDesc = ColumnDescriptor(e.path, selector, extractType(selector, e.data), Ownership(Set(e.tokenId)))
      ProjectionDescriptor(ListMap() + (colDesc -> 0), List[(ColumnDescriptor, SortBy)]() :+ (colDesc, ById)).toOption.get
    }


    "return all selectors for a given path" in {
      val events = sample.get
      val metadata = buildMetadata(events)

      val system = ActorSystem("metadata_test_system")
      val actor = system.actorOf(Props(new ShardMetadataActor(metadata, mutable.Map[Int, Int]())))

      val fut = actor ? FindSelectors(events(0).path)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Seq[JPath]].toSet
      val expected = extractSelectorsFor(events(0).path)(events)
      
      result must_== expected
    }

    "return all metadata for a given (path, selector)" in {
      val events = sample.get
      val metadata = buildMetadata(events)

      val system = ActorSystem("metadata_test_system")
      val actor = system.actorOf(Props(new ShardMetadataActor(metadata, mutable.Map[Int, Int]())))

      val fut = actor ? FindDescriptors(events(0).path, events(0).data.flattenWithPath.head._1)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]]]
      val expected = extractMetadataFor(events(0).path, events(0).data.flattenWithPath.head._1)(events)
     
      result must_== expected
    }
  }
}
