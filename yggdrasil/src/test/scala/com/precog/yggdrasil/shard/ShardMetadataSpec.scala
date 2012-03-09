package com.precog.yggdrasil.shard

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.specification.AfterExample
import org.specs2.specification.BeforeExample
import org.scalacheck._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import org.scalacheck.Gen._

import com.precog.analytics.Path

import com.precog.yggdrasil._

import com.precog.common._
import com.precog.common.util._

import scala.collection.immutable.ListMap

import akka.actor._
import akka.pattern.ask
import akka.util._
import akka.util.duration._
import akka.dispatch._
import akka.testkit._

class ShardMetadataSpec extends Specification with ScalaCheck with RealisticIngestMessage with AfterExample with BeforeExample {

  implicit var actorSystem: ActorSystem = null 
  def before() {
    actorSystem = ActorSystem("test")
  }
  def after() {
    actorSystem.shutdown
  }

  def buildMetadata(sample: List[Event]): Map[ProjectionDescriptor, ColumnMetadata] = {
    def projectionDescriptor(e: Event): Set[ProjectionDescriptor] = { e match {
      case Event(path, tokenId, data, _) => data.flattenWithPath.map {
        case (sel, value) => ColumnDescriptor(path, sel, typeOf(value), Authorities(Set(tokenId)))
      }
    } }.map{ cd => ProjectionDescriptor( ListMap() + (cd -> 0), List[(ColumnDescriptor, SortBy)]() :+ (cd, ById)).toOption.get }.toSet

    def typeOf(jvalue: JValue): ColumnType = {
      ColumnType.forValue(jvalue).getOrElse(SNull)
    }

    def columnMetadata(columns: Seq[ColumnDescriptor]): ColumnMetadata = 
      columns.foldLeft(Map[ColumnDescriptor, MetadataMap]()) { 
        (acc, col) => acc + (col -> Map[MetadataType, Metadata]() ) 
      }

    sample.map(projectionDescriptor).foldLeft( Map[ProjectionDescriptor, ColumnMetadata]()) {
      case (acc, el) => el.foldLeft(acc) {
        case (iacc, pd) => iacc + (pd -> columnMetadata(pd.columns)) 
      }
    }
  }
 
  "ShardMetadata" should {
   
    implicit val eventListArbitrary = Arbitrary(containerOfN[List, Event](50, genEvent))
 
    implicit val timeout = Timeout(Long.MaxValue)
   
    def extractSelectorsFor(path: Path)(events: List[Event]): Set[JPath] = {
      events.flatMap {
        case Event(epath, token, data, metadata) if epath == path => data.flattenWithPath.map(_._1) 
        case _                                                    => List.empty
      }.toSet
    }

    def extractMetadataFor(path: Path, selector: JPath)(events: List[Event]): Map[ProjectionDescriptor, Map[ColumnDescriptor, Map[MetadataType, Metadata]]] = {
      def convertColDesc(cd: ColumnDescriptor) = Map[ColumnDescriptor, Map[MetadataType, Metadata]]() + (cd -> Map[MetadataType, Metadata]())
      def isEqualOrChild(ref: JPath, test: JPath): Boolean = test.nodes.startsWith(ref.nodes) 
      Map(events.flatMap {
        case e @ Event(epath, token, data, metadata) if epath == path => 
          data.flattenWithPath.collect {
            case (k, v) if isEqualOrChild(selector, k) => k
          }.map( toProjectionDescriptor(e, _) )
        case _                                                                              => List.empty
      }.map{ pd => (pd, convertColDesc(pd.columns.head)) }: _*)
    }

    def toProjectionDescriptor(e: Event, selector: JPath) = {
      def extractType(selector: JPath, data: JValue): ColumnType = {
        data.flattenWithPath.find( _._1 == selector).flatMap[ColumnType]( t => ColumnType.forValue(t._2) ).getOrElse(sys.error("bang"))
      }
      
      val colDesc = ColumnDescriptor(e.path, selector, extractType(selector, e.data), Authorities(Set(e.tokenId)))
      ProjectionDescriptor(ListMap() + (colDesc -> 0), List[(ColumnDescriptor, SortBy)]() :+ (colDesc, ById)).toOption.get
    }

    "return all selectors for a given path" ! check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val actor = TestActorRef(new ShardMetadataActor(metadata, VectorClock.empty))

      val fut = actor ? FindSelectors(event.path)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Seq[JPath]].toSet
      
      val expected = extractSelectorsFor(event.path)(sample)
      
      result must_== expected

    }

    "return all metadata for a given (path, selector)" ! check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val actor = TestActorRef(new ShardMetadataActor(metadata, VectorClock.empty))

      val fut = actor ? FindDescriptors(event.path, event.data.flattenWithPath.head._1)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Map[ProjectionDescriptor, Seq[Map[MetadataType, Metadata]]]]

      val expected = extractMetadataFor(event.path, event.data.flattenWithPath.head._1)(sample)
    
      result must_== expected

    }
  }
}
