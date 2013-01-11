package com.precog.yggdrasil
package actor 

import com.precog.common._

import akka.actor.{Actor,ActorRef,Props,Scheduler}
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.pattern.ask
import akka.pattern.gracefulStop
import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration

import blueeyes.json._

import org.slf4j._

import annotation.tailrec
import collection.mutable
import java.util.concurrent.atomic.AtomicLong

import scalaz._
import scalaz.syntax.bind._

//////////////
// MESSAGES //
//////////////

case class GetMessages(sendTo: ActorRef)

sealed trait ShardIngestAction
sealed trait IngestResult extends ShardIngestAction
case class IngestErrors(errors: Seq[String]) extends IngestResult
case class IngestData(messages: Seq[IngestMessage]) extends IngestResult

case class DirectIngestData(messages: Seq[IngestMessage]) extends ShardIngestAction

////////////
// ACTORS //
////////////

/**
 * The purpose of the ingest supervisor is to coordinate multiple sources of data from which data
 * may be ingested. At present, there are two such sources: the inbound kafka queue, represented
 * by the ingestActor, and the "manual" ingest pipeline which may send direct ingest requests to
 * this actor. 
 */
abstract class IngestSupervisor(ingestActorInit: Option[() => Actor], //projectionsActor: ActorRef, routingTable: RoutingTable, 
                                idleDelay: Duration,
                                scheduler: Scheduler,
                                shutdownCheck: Duration) extends Actor {

  protected lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.IngestSupervisor")

  private[this] var ingestActor: Option[ActorRef] = None

  private var initiated = 0
  private var processed = 0
  private var errors = 0

  override def preStart() = {
    ingestActor = ingestActorInit.map { actorInit => context.actorOf(Props(actorInit, "ingestActor")) }
    logger.info("Starting IngestSupervisor against IngestActor " + ingestActor)
    ingestActor.foreach { actor =>
      scheduler.schedule(idleDelay, idleDelay) {
        initiated += 1
        actor ! GetMessages(self)
      }
      logger.info("Recurring ingest request scheduled")
    }
  }

  override def postStop() = {
    logger.info("IngestSupervisor shutting down")
    ingestActor.foreach(gracefulStop(_, shutdownCheck)(context.system))
  }

  def receive = {
    case Status =>
      logger.debug("Ingest supervisor status")
      sender ! status

    case IngestErrors(messages) => 
      errors += 1
      messages.foreach(logger.error(_))

    case IngestData(messages)   => 
      processed += 1
      if (messages.nonEmpty) {
        logger.info("Ingesting " + messages.size + " messages")
        processMessages(messages, sender)
        scheduleIngestRequest(Duration.Zero)
      }

    case DirectIngestData(d) =>
      logger.info("Processing direct ingest of " + d.size + " messages")
      processMessages(d, sender) 
  }

  private def status: JValue = JObject(JField("Routing", JObject(JField("initiated", JNum(initiated)) :: 
                                                                 JField("processed", JNum(processed)) :: Nil)) :: Nil)

  /**
   * This method is responsible for processing a batch of messages, notifying the given coordinator
   * with ProjectionInsertsExpected to set the count, then sending either InsertMetadata or
   * InsertNoMetadata messages after processing each message.
   */
  protected def processMessages(messages: Seq[IngestMessage], batchCoordinator: ActorRef): Unit 

  private def scheduleIngestRequest(delay: Duration): Unit = ingestActor.foreach { actor =>
    initiated += 1
    scheduler.scheduleOnce(delay, actor, GetMessages(self))
  }
}

