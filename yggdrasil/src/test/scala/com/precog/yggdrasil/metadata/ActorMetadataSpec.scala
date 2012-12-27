/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil
package metadata

import actor._
import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.kafka._
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


class ActorMetadataSpec extends Specification with ScalaCheck with RealisticEventMessage with FutureMatchers {
  // TODO: make test more robust about ingest keys
  val ingestAPIKey         = "root"       
  val ingestOwnerAccountId = Some("root") 
  
  trait WithActorSystem extends mutable.Before {
    def before {}
    implicit val actorSystem = ActorSystem("test" + System.nanoTime)
  }

  def buildMetadata(sample: List[Ingest]): Map[ProjectionDescriptor, ColumnMetadata] = {
    def projectionDescriptors(e: Ingest) = {
      for {
        jvalue <- e.data
        (sel, value) <- jvalue.flattenWithPath
      } yield {
        ProjectionDescriptor(1, ColumnDescriptor(e.path, CPath(sel), typeOf(value), Authorities(Set(e.apiKey))) :: Nil)
      }
    }

    def typeOf(jvalue: JValue): CType = {
      CType.forJValue(jvalue).getOrElse(CNull)
    }

    def columnMetadata(columns: Seq[ColumnDescriptor]): ColumnMetadata = {
      columns.foldLeft(Map.empty[ColumnDescriptor, MetadataMap]) { 
        (acc, col) => acc + (col -> Map.empty[MetadataType, Metadata]) 
      }
    }

    sample.map(projectionDescriptors).foldLeft(Map.empty[ProjectionDescriptor, ColumnMetadata]) {
      case (acc, el) => el.foldLeft(acc) {
        case (iacc, pd) => iacc + (pd -> columnMetadata(pd.columns)) 
      }
    }
  }

  implicit val eventListArbitrary = Arbitrary(containerOfN[List, Ingest](50, genIngest))

  implicit val timeout = Timeout(Long.MaxValue)
 
  def extractSelectorsFor(path: Path)(events: List[Ingest]): Set[JPath] = {
    events.flatMap {
      case Ingest(apiKey, epath, None, data, metadata) if epath == path => 
        data flatMap { _.flattenWithPath.map(_._1) }

      case _ => Nil
    }.toSet
  }

  def isEqualOrChild(ref: JPath, test: JPath): Boolean = test.nodes.startsWith(ref.nodes) 
  
  def extractPathsFor(ref: Path)(events: List[Ingest]): Set[Path] = {
    events.collect {
      case Ingest(_, test, _, _, _) if test.isChildOf(ref) => Path(test.elements(ref.length))
    }.toSet
  }

  def extractMetadataFor(path: Path, selector: JPath)(events: List[Ingest]): Map[ProjectionDescriptor, Map[ColumnDescriptor, Map[MetadataType, Metadata]]] = {
    @inline def defaultMetadata(cd: ColumnDescriptor) = Map(cd -> Map.empty[MetadataType, Metadata])

    val descriptors: Set[ProjectionDescriptor] = (events flatMap {
      case e @ Ingest(apiKey, epath, _, data, metadata) if epath == path => 
        data flatMap {
          _.flattenWithPath.collect {
            case (jpath, jv) if isEqualOrChild(selector, jpath) => 
              val cd = ColumnDescriptor(epath, CPath(jpath), CType.forJValue(jv).get, Authorities(Set(apiKey)))
              ProjectionDescriptor(1, cd :: Nil)
          }
        }

      case _ => List.empty
    })(scala.collection.breakOut) 
    
    descriptors map { pd => (pd, defaultMetadata(pd.columns.head)) } toMap
  }
  
  /*
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
  */

  "ShardMetadata" should {
    "return all children for the root path" ! new WithActorSystem { check { (sample: List[Ingest]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)
      
      val actor = TestActorRef(new MetadataActor("ActorMetadataSpec", new TestMetadataStorage(metadata), CheckpointCoordination.Noop, None))
      val expected = extractPathsFor(Path.Root)(sample)

      (actor ? FindChildren(Path.Root)) must whenDelivered {
        be_==(expected)
      }
    }}
    
    "return all children for the an arbitrary path" ! new WithActorSystem { check { (sample: List[Ingest]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val testPath: Path = event.path.parent.getOrElse(event.path)

      val actor = TestActorRef(new MetadataActor("ActorMetadataSpec", new TestMetadataStorage(metadata), CheckpointCoordination.Noop, None))
      val expected = extractPathsFor(testPath)(sample)

      (actor ? FindChildren(testPath)) must whenDelivered {
        be_==(expected)
      }
    }}

    "return all selectors for a given path" ! new WithActorSystem { check { (sample: List[Ingest]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val actor = TestActorRef(new MetadataActor("ActorMetadataSpec", new TestMetadataStorage(metadata), CheckpointCoordination.Noop, None))
      val expected = extractSelectorsFor(event.path)(sample)

      (actor ? FindSelectors(event.path)) must whenDelivered {
        be_==(expected)
      }
    }}

    "return all metadata for a given (path, selector)" ! new WithActorSystem { check { (sample: List[Ingest]) =>
      val metadata = buildMetadata(sample)
      val event = sample(0)

      val sampleJPath = event.data.flatMap(_.flattenWithPath).head._1

      val actor = TestActorRef(new MetadataActor("ActorMetadataSpec", new TestMetadataStorage(metadata), CheckpointCoordination.Noop, None))

      val expected = extractMetadataFor(event.path, sampleJPath)(sample)

      (actor ? FindDescriptors(event.path, CPath(sampleJPath))) must whenDelivered {
        be_==(expected)
      }
    }}
  }
}
