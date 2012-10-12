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
package com.precog.performance

import org.specs2.mutable.Specification

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._

import akka.actor._
import akka.pattern.ask
import akka.dispatch._
import akka.util._
import akka.util.Duration

import blueeyes.json.JsonAST._

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

trait RoutingPerformanceSpec extends Specification with PerformanceSpec {
  "routing actor" should {
    
    "route" in { 
      implicit val stopTimeout: Timeout = Duration(60, "seconds")

      val benchParams = BenchmarkParameters(5, 500, Some(500), false)
      val singleParams = BenchmarkParameters(5, 500, Some(500), false)

      val inserts = 100000

      val system = ActorSystem("routingActorTest")

      val batchSize = 100

      val sampler = DistributedSampleSet(0, sampler = AdSamples.adCampaignSample)

      val samples = 0.until(batchSize) map { _ =>
        sampler.next._1
      }

      val seq = new AtomicInteger(0)

      val batch: Seq[IngestMessage] = samples map 
           { jval => Event(Path("/"), "apiKey", jval, Map()) } map 
           { event => EventMessage(0, seq.getAndIncrement, event) }

 
      val metadataActor: ActorRef = 
        system.actorOf(Props(new MockMetadataActor()), "mock_metadata_actor")

      val projectionActor: ActorRef = 
        system.actorOf(Props(new MockProjectionActor), "mock_projection_actor")
 
      val projectionActors: ActorRef = 
        system.actorOf(Props(new MockProjectionActors(projectionActor)), "mock_projections_actor")

      val routingTable: RoutingTable = new SingleColumnProjectionRoutingTable
      
      val ingestActor: ActorRef = 
        system.actorOf(Props(new MockIngestActor(inserts / batchSize, batch)), "mock_shard_ingest")
      
      val routingActor: ActorRef = {
        val routingTable = new SingleColumnProjectionRoutingTable
        val eventStore = new EventStore(routingTable, projectionActors, metadataActor, Duration(60, "seconds"), new Timeout(60000), ExecutionContext.defaultExecutionContext(system))
        system.actorOf(Props(new BatchStoreActor(eventStore, 1000, Some(ingestActor), system.scheduler)), "router") 
      }
     
      def testIngest() = {
          val barrier = new CountDownLatch(1)

          ingestActor ! MockIngestReset(barrier)
          routingActor ! Start 
          
          barrier.await

          val fut = routingActor ? ControlledStop
          
          Await.result(fut, Duration(60, "seconds"))      
      }

      try {
        println("routing actor performance")
        val result = Performance().benchmark(testIngest(), benchParams, benchParams)
        //perfUtil.uploadResults("routing actor", result)
        //val result = Performance().profile(testIngest())   
 
        result.report("routing actor", System.out)
        
        true must_== true
      } finally {
        system.shutdown
      }
    }
  }
}

case class MockIngestReset(barrier: CountDownLatch)

class MockMetadataActor extends Actor {
  def receive = {
    case UpdateMetadata(_) => sender ! ()
    case _                 => println("Unplanned metadata actor action")
  }
}

class MockIngestActor(toSend: Int, messageBatch: Seq[IngestMessage]) extends Actor {
  private var barrier: CountDownLatch = null
  private var sent = 0

  def receive = {
    case MockIngestReset(b) => 
      barrier = b
      sent = 0
    case GetMessages(replyTo) =>
      sent += 1
      if(sent < toSend) {
        replyTo ! IngestData(messageBatch) 
      } else {
        barrier.countDown
        replyTo ! NoIngestData
      }
    case ()                    =>

    case x                     =>  println("Unplanned ingest actor action: " + x.getClass.getName)
  }
}

class MockProjectionActors(projectionActor: ActorRef) extends Actor {
  def receive = {
    case AcquireProjection(desc) =>
      sender ! ProjectionAcquired(projectionActor)
    case AcquireProjectionBatch(descs) =>
      var map = Map.empty[ProjectionDescriptor, ActorRef]
      val descItr = descs.iterator
      while(descItr.hasNext) {
        map += (descItr.next -> projectionActor)
      }
      sender ! ProjectionBatchAcquired(map)
    case ReleaseProjection(_) =>
    case ReleaseProjectionBatch(_) => sender ! ()
    case _                     =>  println("Unplanned projection actors action")
  } 
}

class MockProjectionActor extends Actor {
  def receive = {
    case ProjectionInsert(_,_) =>
      sender ! ()
    case ProjectionBatchInsert(_) =>
      sender ! ()
    case _                     =>  println("Unplanned projection actor action")
  }
}
