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
package actor 

import metadata._

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil._

import blueeyes.json.JPath

import org.specs2.mutable._

import akka.pattern.ask
import akka.actor._
import akka.dispatch._
import akka.util._

import scala.collection.immutable.ListMap

object ShardMetadataActorSpec extends Specification {

  val system = ActorSystem("shard_metadata_test")
  implicit val timeout = Timeout(30000) 

  "shard metadata actor" should {
    "correctly propagates initial message clock on flush request" in {
      val testActor = system.actorOf(Props(new TestShardMetadataActor), "test-metadata-actor1")
      val captureActor = system.actorOf(Props(new CaptureActor), "test-capture-actor1") 
      
      val fut1 = testActor ? FlushMetadata(captureActor)

      val fut2 = fut1 flatMap { _ => captureActor ? GetCaptureResult }

      val (save, other) = Await.result(fut2, Duration(30, "seconds")).asInstanceOf[(Vector[SaveMetadata],Vector[Any])]

      other.size must_== 0
      save must_== Vector(SaveMetadata(Map(), VectorClock.empty.update(0,0)))

    }
    "correctly propagates updated message clock on flush request" in {
      val testActor = system.actorOf(Props(new TestShardMetadataActor), "test-metadata-actor2")
      val captureActor = system.actorOf(Props(new CaptureActor), "test-capture-actor2") 

      val colDesc = ColumnDescriptor(Path("/"), JPath(".test"), SStringArbitrary, Authorities(Set("me")))

      val indexedColumns = ListMap((colDesc -> 0))
      val sorting = Vector((colDesc -> ById))

      val descriptor = ProjectionDescriptor(indexedColumns, sorting).toOption.get
      val values = Vector[CValue](CString("Test123"))
      val metadata = Vector(Set[Metadata]())

      val insertComplete1 = InsertComplete(EventId(0,1), descriptor, values, metadata)
      val insertComplete2 = InsertComplete(EventId(0,2), descriptor, values, metadata)

      val inserts = List[InsertComplete](insertComplete1, insertComplete2)
      
      val fut0 = testActor ? UpdateMetadata(inserts)
   
      val fut1 = fut0 flatMap { _ => testActor ? FlushMetadata(captureActor) }

      val fut2 = fut1 flatMap { _ => captureActor ? GetCaptureResult }

      val (save, other) = Await.result(fut2, Duration(30, "seconds")).asInstanceOf[(Vector[SaveMetadata],Vector[Any])]

      val stringStats = StringValueStats(2, "Test123", "Test123")

      val resultingMetadata = Map(
        (descriptor -> Map[ColumnDescriptor, MetadataMap]((colDesc -> Map((stringStats.metadataType -> stringStats)))))
      )

      other.size must_== 0
      save must_== Vector(SaveMetadata(resultingMetadata, VectorClock.empty.update(0,2)))
    }
  }

  step {
    system.shutdown
  }
}

class TestShardMetadataActor extends ShardMetadataActor(Map(), VectorClock.empty.update(0,0))

case object GetCaptureResult

class CaptureActor extends Actor {

  var saveMetadataCalls = Vector[SaveMetadata]()
  var otherCalls = Vector[Any]()

  def receive = {
    case sm @ SaveMetadata(_, _) => 
      saveMetadataCalls = saveMetadataCalls :+ sm
    case GetCaptureResult => sender ! (saveMetadataCalls, otherCalls)
    case other                   => 
      otherCalls = otherCalls :+ other
  }
}
