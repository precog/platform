package com.precog.yggdrasil
package actor 

import com.precog.common._

import akka.actor.Actor
import akka.actor.Scheduler
import akka.actor.ActorRef
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s._

import annotation.tailrec
import collection.mutable
import java.util.concurrent.atomic.AtomicLong

import scalaz._
import scalaz.syntax.bind._

//////////////
// MESSAGES //
//////////////

case class GetMessages(sendTo: ActorRef)

sealed trait IngestResult
case class IngestErrors(errors: Seq[String]) extends IngestResult
case class IngestData(messages: Seq[IngestMessage]) extends IngestResult

case class DirectIngestData(messages: Seq[IngestMessage])

////////////
// ACTORS //
////////////

/**
 * The purpose of the ingest supervisor is to coordinate multiple sources of data from which data
 * may be ingested. At present, there are two such sources: the inbound kafka queue, represented
 * by the ingestActor, and the "manual" ingest pipeline which may send direct ingest requests to
 * this actor. 
 */
class IngestSupervisor(ingestActor: ActorRef, projectionsActor: ActorRef, routingTable: RoutingTable, 
                       idleDelay: Duration, scheduler: Scheduler, shutdownCheck: Duration) extends Actor with Logging {

  private var initiated = 0
  private var processed = 0
  private var errors = 0

  override def preStart() = {
    scheduleIngestRequest(Duration.Zero)
  }

  def receive = {
    case Status =>
      sender ! status

    case IngestErrors(messages) => 
      errors += 1
      messages.foreach(logger.error(_))
      scheduleIngestRequest(idleDelay)

    case IngestData(messages)   => 
      processed += 1
      if (messages.isEmpty) {
        scheduleIngestRequest(idleDelay)
      } else {
        processMessages(messages, sender)
        scheduleIngestRequest(Duration.Zero)
      }

    case DirectIngestData(d) =>
      processMessages(d, sender) 
  }

  private def status: JValue = JObject(JField("Routing", JObject(JField("initiated", JInt(initiated)) :: 
                                                                 JField("processed", JInt(processed)) :: Nil)) :: Nil)

  private def processMessages(messages: Seq[IngestMessage], coordinator: ActorRef): Unit = {
    val inserts = routingTable.batchMessages(messages)

    coordinator ! ProjectionInsertsExpected(inserts.size)
    for (insert <- inserts) projectionsActor.tell(insert, coordinator)
  }

  private def scheduleIngestRequest(delay: Duration): Unit = {
    initiated += 1
    scheduler.scheduleOnce(delay, ingestActor, GetMessages)
  }
}

