package com.precog.yggdrasil
package metadata

import actor._
import com.precog.common._
import com.precog.common.util._
import com.precog.common.json._
import com.precog.util._

import blueeyes.akka_testing._
import blueeyes.json._

import akka.actor._
import akka.pattern.ask
import akka.util._
import akka.util.duration._
import akka.dispatch._
import akka.testkit._

import scala.collection.immutable.ListMap

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.specification.Scope
import org.scalacheck._
import org.scalacheck.Gen._


class ActorMetadataSpec extends Specification with ScalaCheck with RealisticIngestMessage with FutureMatchers {
  
  val ingestAPIKey         = "root"       // FIXME
  val ingestOwnerAccountId = Some("root") // FIXME
  
  trait WithActorSystem extends mutable.Before {
    def before {}
    implicit val actorSystem = ActorSystem("test" + System.nanoTime)
  }

  def buildMetadata(sample: List[Event]): Map[ProjectionDescriptor, ColumnMetadata] = {
    def projectionDescriptors(e: Event) = {
      e.data.flattenWithPath.map {
        case (sel, value) => ProjectionDescriptor(1, ColumnDescriptor(e.path, CPath(sel), typeOf(value), Authorities(Set(e.apiKey))) :: Nil)
      }
    }

    def typeOf(jvalue: JValue): CType = {
      CType.forJValue(jvalue).getOrElse(CNull)
    }

    def columnMetadata(columns: Seq[ColumnDescriptor]): ColumnMetadata = 
      columns.foldLeft(Map[ColumnDescriptor, MetadataMap]()) { 
        (acc, col) => acc + (col -> Map[MetadataType, Metadata]() ) 
      }

    sample.map(projectionDescriptors).foldLeft( Map[ProjectionDescriptor, ColumnMetadata]()) {
      case (acc, el) => el.foldLeft(acc) {
        case (iacc, pd) => iacc + (pd -> columnMetadata(pd.columns)) 
      }
    }
  }

  implicit val eventListArbitrary = Arbitrary(containerOfN[List, Event](50, genEvent))

  implicit val timeout = Timeout(Long.MaxValue)
 
  def extractSelectorsFor(path: Path)(events: List[Event]): Set[JPath] = {
    events.flatMap {
      case Event(apiKey, epath, None, data, metadata) if epath == path => data.flattenWithPath.map(_._1) 
      case _                                                    => List.empty
    }.toSet
  }

  def isEqualOrChild(ref: JPath, test: JPath): Boolean = test.nodes.startsWith(ref.nodes) 
  
  def extractPathsFor(ref: Path)(events: List[Event]): Set[Path] = {
    events.collect {
      case Event(_, test, _, _, _) if test.isChildOf(ref) => Path(test.elements(ref.length))
    }.toSet
  }

  def extractMetadataFor(path: Path, selector: JPath)(events: List[Event]): Map[ProjectionDescriptor, Map[ColumnDescriptor, Map[MetadataType, Metadata]]] = {
    def convertColDesc(cd: ColumnDescriptor) = Map[ColumnDescriptor, Map[MetadataType, Metadata]]() + (cd -> Map[MetadataType, Metadata]())
    Map(events.flatMap {
      case e @ Event(apiKey, epath, _, data, metadata) if epath == path => 
        data.flattenWithPath.collect {
          case (k, v) if isEqualOrChild(selector, k) => k
        }.map( toProjectionDescriptor(e, _) )
      case _                                                        => List.empty
    }.map{ pd => (pd, convertColDesc(pd.columns.head)) }: _*)
  }
  
  trait ChildType
  case class LeafChild(cType: CType, apiKey: String) extends ChildType
  case class IndexChild(i: Int) extends ChildType
  case class FieldChild(n: String) extends ChildType
  case object NotChild extends ChildType
  
  def projectionDescriptorMap(path: Path, selector: CPath, cType: CType, apiKey: String) = {
    val colDesc = ColumnDescriptor(path, selector, cType, Authorities(Set(apiKey)))
    val desc = ProjectionDescriptor(1, colDesc :: Nil)
    val metadata = Map[ColumnDescriptor, Map[MetadataType, Metadata]]() + (colDesc -> Map[MetadataType, Metadata]())
    Map((desc -> metadata))
  }

  def extractPathMetadata(path: Path, selector: JPath, in: Set[(JPath, CType, String)]): Set[PathMetadata] = {
    def classifyChild(ref: JPath, test: JPath, cType: CType, apiKey: String): ChildType = {
      if((test.nodes startsWith ref.nodes) && (test.nodes.length > ref.nodes.length)) {
        if(test.nodes.length - 1 == ref.nodes.length) {
          LeafChild(cType, apiKey)
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
      case (sel, cType, apiKey) => classifyChild(selector, sel, cType, apiKey)
    } 

    val c2 = c1.filter { 
      case NotChild => false
      case _        => true
    }
    val classifiedChildren = c2

    classifiedChildren map {
      case LeafChild(cType, apiKey)  => 
        PathValue(cType, Authorities(Set(apiKey)), projectionDescriptorMap(path, CPath(selector), cType, apiKey))
      case IndexChild(i) =>
        PathIndex(i, extractPathMetadata(path, selector \ i, in))
      case FieldChild(n) =>
        PathField(n, extractPathMetadata(path, selector \ n, in))
    }
  }

  def extractPathMetadataFor(path: Path, selector: JPath)(events: List[Event]): PathRoot = {
    val col: Set[(JPath, CType, String)] = events.collect {
      case Event(apiKey, `path`, _, data, _) =>
        data.flattenWithPath.collect {
          case (s, v) if isEqualOrChild(selector, s) => 
             val ns = s.nodes.slice(selector.length, s.length-1)
            (JPath(ns), CType.forJValue(v).get, apiKey)
        }
    }.flatten.toSet

    PathRoot(extractPathMetadata(path, JPath(""), col)) 
  }

  def toProjectionDescriptor(e: Event, selector: JPath) = {
    def extractType(selector: JPath, data: JValue): CType = {
      data.flattenWithPath.find( _._1 == selector).flatMap[CType]( t => CType.forJValue(t._2) ).getOrElse(sys.error("bang"))
    }
    
    val colDesc = ColumnDescriptor(e.path, CPath(selector), extractType(selector, e.data), Authorities(Set(e.apiKey)))
    ProjectionDescriptor(1, colDesc :: Nil)
  }
    
  "ShardMetadata" should {
    "return all children for the root path" ! new WithActorSystem { check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)
      
      val actor = TestActorRef(new MetadataActor("ActorMetadataSpec", new TestMetadataStorage(metadata), CheckpointCoordination.Noop, None, false))
      val expected = extractPathsFor(Path.Root)(sample)

      (actor ? FindChildren(Path.Root)) must whenDelivered {
        be_==(expected)
      }
    }}
    
    "return all children for the an arbitrary path" ! new WithActorSystem { check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val testPath: Path = event.path.parent.getOrElse(event.path)

      val actor = TestActorRef(new MetadataActor("ActorMetadataSpec", new TestMetadataStorage(metadata), CheckpointCoordination.Noop, None, false))
      val expected = extractPathsFor(testPath)(sample)

      (actor ? FindChildren(testPath)) must whenDelivered {
        be_==(expected)
      }
    }}

    "return all selectors for a given path" ! new WithActorSystem { check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val actor = TestActorRef(new MetadataActor("ActorMetadataSpec", new TestMetadataStorage(metadata), CheckpointCoordination.Noop, None, false))
      val expected = extractSelectorsFor(event.path)(sample)

      (actor ? FindSelectors(event.path)) must whenDelivered {
        be_==(expected)
      }
    }}

    "return all metadata for a given (path, selector)" ! new WithActorSystem { check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val actor = TestActorRef(new MetadataActor("ActorMetadataSpec", new TestMetadataStorage(metadata), CheckpointCoordination.Noop, None, false))
      val expected = extractMetadataFor(event.path, event.data.flattenWithPath.head._1)(sample)

      (actor ? FindDescriptors(event.path, CPath(event.data.flattenWithPath.head._1))) must whenDelivered {
        be_==(expected)
      }
    }}
  }
}
