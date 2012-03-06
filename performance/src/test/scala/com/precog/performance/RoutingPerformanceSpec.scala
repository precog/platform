package com.precog.performance

import org.specs2.mutable.Specification

import com.precog.analytics.Path
import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil.kafka._
import com.precog.yggdrasil.shard._

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
    
    "route 10K elements in 750ms".performBatch(10000, 750) { inserts =>

      implicit val stopTimeout: Timeout = Duration(60, "seconds")

      val system = ActorSystem("routing_actor_test")

      val batchSize = 100

      val sampler = DistributedSampleSet(0, sampler = AdSamples.adCampaignSample)

      val samples = 0.until(batchSize) map { _ =>
        sampler.next._1
      }

      val seq = new AtomicInteger(0)

      val batch: Seq[IngestMessage] = samples map 
           { jval => Event(Path("/"), "token", jval, Map()) } map 
           { event => EventMessage(0, seq.getAndIncrement, event) }

      val barrier = new CountDownLatch(1)
 
      val ingestActor: ActorRef = 
        system.actorOf(Props(new MockIngestActor(inserts / batchSize, barrier, batch)), "mock_shard_ingest")
     
      val metadataActor: ActorRef = 
        system.actorOf(Props(new MockMetadataActor()), "mock_metadata_actor")

      val projectionActor: ActorRef = 
        system.actorOf(Props(new MockProjectionActor), "mock_projection_actor")
 
      val projectionActors: ActorRef = 
        system.actorOf(Props(new MockProjectionActors(projectionActor)), "mock_projections_actor")

      val routingTable: RoutingTable = AltSingleColumnProjectionRoutingTable
      val routingActor: ActorRef = 
        system.actorOf(Props(new RoutingActor(routingTable, ingestActor, projectionActors, metadataActor, system.scheduler, Duration(5, "millis"))), "router")
    
      val start = System.nanoTime
 
      routingActor ! CheckMessages
      
      barrier.await

      val fut = routingActor ? ControlledStop
      
      Await.result(fut, Duration(60, "seconds"))      
      
      val finish = System.nanoTime
      system.shutdown
    }
  }
}

class MockMetadataActor extends Actor {
  def receive = {
    case UpdateMetadata(_) =>
    case _                 => println("Unplanned metadata actor action")
  }
}

class MockIngestActor(toSend: Int, barrier: CountDownLatch, messageBatch: Seq[IngestMessage]) extends Actor {

  private var sent = 0

  def receive = {
    case GetMessages(replyTo) =>
      sent += 1
      if(sent < toSend) {
        replyTo ! Messages(messageBatch) 
      } else {
        barrier.countDown
        replyTo ! NoMessages
      }
    case x                     =>  println("Unplanned ingest actor action: " + x.getClass.getName)
  }
}

class MockProjectionActors(projectionActor: ActorRef) extends Actor {
  def receive = {
    case AcquireProjection(desc) =>
      sender ! ProjectionAcquired(projectionActor)
    case ReleaseProjection(_) =>
    case _                     =>  println("Unplanned projection actors action")
  } 
}

class MockProjectionActor extends Actor {
  def receive = {
    case ProjectionInsert(_,_) =>
      sender ! ()
    case _                     =>  println("Unplanned projection actor action")
  }
}
