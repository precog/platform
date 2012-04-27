package com.precog.yggdrasil
package metadata

import actor._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.specification.AfterExample
import org.specs2.specification.BeforeExample
import org.scalacheck._

import blueeyes.json.JPath
import blueeyes.json.JPathField
import blueeyes.json.JPathIndex
import blueeyes.json.JsonAST._

import org.scalacheck.Gen._

import com.precog.common._
import com.precog.common.util._

import scala.collection.immutable.ListMap

import akka.actor._
import akka.pattern.ask
import akka.util._
import akka.util.duration._
import akka.dispatch._
import akka.testkit._

class ActorMetadataSpec extends Specification with ScalaCheck with RealisticIngestMessage with AfterExample with BeforeExample {

  implicit var actorSystem: ActorSystem = null 
  def before() {
    actorSystem = ActorSystem("test")
  }
  def after() {
    actorSystem.shutdown
  }

  def buildMetadata(sample: List[Event]): Map[ProjectionDescriptor, ColumnMetadata] = {
    def projectionDescriptor(ev: Event): Set[ProjectionDescriptor] = { 
      ev.data.flattenWithPath.map {
        case (sel, value) => ProjectionDescriptor(1, List(ColumnDescriptor(ev.path, sel, typeOf(value), Authorities(Set(ev.tokenId)))))
      } toSet
    }

    def typeOf(jvalue: JValue): CType = {
      CType.forJValue(jvalue).getOrElse(CNull)
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

    def isEqualOrChild(ref: JPath, test: JPath): Boolean = test.nodes.startsWith(ref.nodes) 
    
    def extractPathsFor(ref: Path)(events: List[Event]): Set[Path] = {

      def isDescendant(ref: Path, test: Path): Boolean = 
        test.elements.startsWith(ref.elements) &&
        test.elements.length > ref.elements.length
      
      events.collect {
        case Event(test, _, _, _) if isDescendant(ref, test) => Path(test.elements(ref.length))
      }.toSet
    }

    def extractMetadataFor(path: Path, selector: JPath)(events: List[Event]): Map[ProjectionDescriptor, Map[ColumnDescriptor, Map[MetadataType, Metadata]]] = {
      def convertColDesc(cd: ColumnDescriptor) = Map[ColumnDescriptor, Map[MetadataType, Metadata]]() + (cd -> Map[MetadataType, Metadata]())
      Map(events.flatMap {
        case e @ Event(epath, token, data, metadata) if epath == path => 
          data.flattenWithPath.collect {
            case (k, v) if isEqualOrChild(selector, k) => k
          }.map( toProjectionDescriptor(e, _) )
        case _                                                        => List.empty
      }.map{ pd => (pd, convertColDesc(pd.columns.head)) }: _*)
    }
    
    trait ChildType
    case class LeafChild(cType: CType, token: String) extends ChildType
    case class IndexChild(i: Int) extends ChildType
    case class FieldChild(n: String) extends ChildType
    case object NotChild extends ChildType
    
    def projectionDescriptorMap(path: Path, selector: JPath, cType: CType, token: String) = {
      val colDesc = ColumnDescriptor(path, selector, cType, Authorities(Set(token)))
      val desc = ProjectionDescriptor(1, List(colDesc))
      val metadata = Map[ColumnDescriptor, Map[MetadataType, Metadata]]() + (colDesc -> Map[MetadataType, Metadata]())
      Map((desc -> metadata))
    }

    def extractPathMetadata(path: Path, selector: JPath, in: Set[(JPath, CType, String)]): Set[PathMetadata] = {
 

      def classifyChild(ref: JPath, test: JPath, cType: CType, token: String): ChildType = {
        if((test.nodes startsWith ref.nodes) && (test.nodes.length > ref.nodes.length)) {
          if(test.nodes.length - 1 == ref.nodes.length) {
            LeafChild(cType, token)
          } else {
            test.nodes(ref.nodes.length) match {
              case JPathField(i) => FieldChild(i)
              case JPathIndex(s) => IndexChild(s)
              case _             => NotChild
            }
          }
        } else {
          NotChild
        }
      }

      val c1 = in map {
        case (sel, cType, token) => classifyChild(selector, sel, cType, token)
      } 

      val c2 = c1.filter { 
        case NotChild => false
        case _        => true
      }
      val classifiedChildren = c2

      classifiedChildren map {
        case LeafChild(cType, token)  => 
          PathValue(cType, projectionDescriptorMap(path, selector, cType, token))
        case IndexChild(i) =>
          PathIndex(i, extractPathMetadata(path, selector \ i, in))
        case FieldChild(n) =>
          PathField(n, extractPathMetadata(path, selector \ n, in))
      }
    }

    def extractPathMetadataFor(path: Path, selector: JPath)(events: List[Event]): PathRoot = {
      val col: Set[(JPath, CType, String)] = events.collect {
        case Event(`path`, token, data, _) =>
          data.flattenWithPath.collect {
            case (s, v) if isEqualOrChild(selector, s) => 
               val ns = s.nodes.slice(selector.length, s.length-1)
              (JPath(ns), CType.forJValue(v).get, token)
          }
      }.flatten.toSet

      PathRoot(extractPathMetadata(path, JPath(""), col)) 
    }

    def toProjectionDescriptor(e: Event, selector: JPath) = {
      def extractType(selector: JPath, data: JValue): CType = {
        data.flattenWithPath.find( _._1 == selector).flatMap[CType]( t => CType.forJValue(t._2) ).getOrElse(sys.error("bang"))
      }
      
      val colDesc = ColumnDescriptor(e.path, selector, extractType(selector, e.data), Authorities(Set(e.tokenId)))
      ProjectionDescriptor(1, List(colDesc))
    }
    
    "return all children for the root path" ! check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)
      
      val actor = TestActorRef(new MetadataActor(new LocalMetadata(metadata, VectorClock.empty)))

      val fut = actor ? FindChildren(Path(""))

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Seq[Path]].toSet
      
      val expected = extractPathsFor(Path(""))(sample)
      
      result must_== expected

    }
    
    "return all children for the an arbitrary path" ! check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val testPath: Path = event.path.parent.getOrElse(event.path)

      val actor = TestActorRef(new MetadataActor(new LocalMetadata(metadata, VectorClock.empty)))

      val fut = actor ? FindChildren(testPath)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Seq[Path]].toSet
      
      val expected = extractPathsFor(testPath)(sample)
      
      result must_== expected

    }

    "return all selectors for a given path" ! check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val actor = TestActorRef(new MetadataActor(new LocalMetadata(metadata, VectorClock.empty)))

      val fut = actor ? FindSelectors(event.path)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Seq[JPath]].toSet
      
      val expected = extractSelectorsFor(event.path)(sample)
      
      result must_== expected

    }

    "return all metadata for a given (path, selector)" ! check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val actor = TestActorRef(new MetadataActor(new LocalMetadata(metadata, VectorClock.empty)))

      val fut = actor ? FindDescriptors(event.path, event.data.flattenWithPath.head._1)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Map[ProjectionDescriptor, Seq[Map[MetadataType, Metadata]]]]

      val expected = extractMetadataFor(event.path, event.data.flattenWithPath.head._1)(sample)
    
      result must_== expected

    }
   
    "return all metadata for a given (path, selector)" ! check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val actor = TestActorRef(new MetadataActor(new LocalMetadata(metadata, VectorClock.empty)))

      val fut = actor ? FindPathMetadata(event.path, event.data.flattenWithPath.head._1)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[PathRoot]
    
      val expected = extractPathMetadataFor(event.path, event.data.flattenWithPath.head._1)(sample)
      
      result must_== expected

    }.pendingUntilFixed

  }
}
