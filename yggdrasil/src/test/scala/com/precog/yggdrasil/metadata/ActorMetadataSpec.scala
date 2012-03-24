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

    def isEqualOrChild(ref: JPath, test: JPath): Boolean = test.nodes.startsWith(ref.nodes) 

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
    case object LeafChild extends ChildType
    case object IndexChild extends ChildType
    case object FieldChild extends ChildType
    case object NotChild extends ChildType

    def extractPathMetadata(in: Set[(JPath, ColumnType)]): Set[PathMetadata] = {
   
      def classifyChild(ref: JPath, test: JPath): ChildType =
        if(test.nodes startsWith ref.nodes) {
          if(test.nodes.length - 1 == ref.nodes.length) {
            LeafChild
          } else {
            test.nodes(ref.nodes.length) match {
              case JPathField(_) => FieldChild
              case JPathIndex(_) => IndexChild
              case _             => NotChild
            }
          }
        } else {
          NotChild
        }
      sys.error("todo")
    }

    def extractPathMetadataFor(path: Path, selector: JPath)(events: List[Event]): PathRoot = {
      val col: Set[(JPath, ColumnType)] = events.collect {
        case Event(`path`, token, data, metadata) =>
          data.flattenWithPath.collect {
            case (s, v) if isEqualOrChild(selector, s) => 
               val ns = s.nodes.slice(selector.length, s.length-1)
              (JPath(ns), ColumnType.forValue(v).get)
          }
      }.flatten.toSet

      PathRoot(extractPathMetadata(col)) 
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
