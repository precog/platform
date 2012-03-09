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
package com.precog.yggdrasil.shard

import org.specs2._
import org.specs2.mutable.Specification
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

class ShardMetadataSpec extends Specification with ScalaCheck with RealisticIngestMessage {
 
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

      val system = ActorSystem("metadata_test_system")
      val actor = system.actorOf(Props(new ShardMetadataActor(metadata, VectorClock.empty)), "metadata_test")

      val fut = actor ? FindSelectors(sample(0).path)
      

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Seq[JPath]].toSet
      
      system.shutdown
     
      val expected = extractSelectorsFor(sample(0).path)(sample)
      
      result must_== expected
    }

    "return all metadata for a given (path, selector)" ! check { (sample: List[Event]) =>
      val metadata = buildMetadata(sample)

      val system = ActorSystem("metadata_test_system")
      val actor = system.actorOf(Props(new ShardMetadataActor(metadata, VectorClock.empty)), "metadata_test")

      val fut = actor ? FindDescriptors(sample(0).path, sample(0).data.flattenWithPath.head._1)

      val result = Await.result(fut, Duration(30,"seconds")).asInstanceOf[Map[ProjectionDescriptor, Seq[Map[MetadataType, Metadata]]]]
      
      system.shutdown
     
      val expected = extractMetadataFor(sample(0).path, sample(0).data.flattenWithPath.head._1)(sample)
    
      result must_== expected

    }
  }
}
