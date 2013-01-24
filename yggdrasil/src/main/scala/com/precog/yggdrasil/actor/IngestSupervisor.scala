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
abstract class IngestSupervisor(ingestActor: Option[ActorRef], 
                                projectionsActor: ActorRef,
                                idleDelay: Duration,
                                scheduler: Scheduler,
                                shutdownCheck: Duration) extends Actor {

  protected lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.IngestSupervisor")

  private val routingTable = new SingleColumnProjectionRoutingTable

  private var initiated = 0
  private var processed = 0
  private var errors = 0

  override def preStart() = {
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
  def processMessages(messages: Seq[IngestMessage], batchCoordinator: ActorRef): Unit = {
    logger.debug("Beginning processing of %d messages".format(messages.size))
    implicit val to = yggConfig.metadataTimeout
    implicit val execContext = ExecutionContext.defaultExecutionContext(ingestActorSystem)
    
    val archivePaths = messages.collect { case ArchiveMessage(_, Archive(path, _)) => path } 

    if (archivePaths.nonEmpty) {
      logger.debug("Processing archive paths: " + archivePaths)
    } else {
      logger.debug("No archive paths")
    }

    Future.sequence {
      archivePaths map { path =>
        (metadataActor ? FindDescriptors(path, CPath.Identity)).mapTo[Set[ProjectionDescriptor]]
      }
    }.onSuccess {
      case descMaps : Seq[Set[ProjectionDescriptor]] => 
        val projectionMap: Map[Path, Seq[ProjectionDescriptor]] = (for {
          descMap <- descMaps
          desc    <- descMap
          column  <- desc.columns
        } yield (column.path, desc)).groupBy(_._1).mapValues(_.map(_._2))

        projectionMap.foreach { case (p,d) => logger.debug("Archiving %d projections on path %s".format(d.size, p)) }
      
        val updates = routingTable.batchMessages(messages, projectionMap)

        logger.debug("Sending " + updates.size + " update message(s)")
        batchCoordinator ! ProjectionUpdatesExpected(updates.size)
        for (update <- updates) projectionsActor.tell(update, batchCoordinator)
    }
  }

  private def scheduleIngestRequest(delay: Duration): Unit = ingestActor.foreach { actor =>
    initiated += 1
    scheduler.scheduleOnce(delay, actor, GetMessages(self))
  }
}

