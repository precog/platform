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

import metadata.ColumnMetadata
import com.precog.util._
import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.kafka._
import com.precog.common.security._
import ColumnMetadata.monoid

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.PoisonPill
import akka.dispatch.{Await, ExecutionContext, Future, Promise}
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout

import com.weiglewilczek.slf4s._
import org.slf4j._

import java.util.concurrent.TimeUnit.SECONDS

import _root_.kafka.api.FetchRequest
import _root_.kafka.consumer.SimpleConsumer
import _root_.kafka.message.MessageSet

import blueeyes.json._
import blueeyes.json.serialization.Extractor.Error
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import Validation._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.TreeMap

//////////////
// MESSAGES //
//////////////

case class ProjectionUpdatesExpected(projections: Int)

////////////
// ACTORS //
////////////

/**
 * A shard ingest actor manages the IO with an external sources of messages. Its responsibilities are:
 * 1) Manage the state of the connection to the external system
 * 2) Update the system of record with information needed to reconcile the state of the
 *    external system with the state of the system into which data is being ingested. For Kafka,
 *    the most important component of this state is the offset.
 */
abstract class KafkaShardIngestActor(shardId: String,
                                     initialCheckpoint: YggCheckpoint,
                                     consumer: SimpleConsumer,
                                     topic: String,
                                     ingestEnabled: Boolean,
                                     accountFinder: AccountFinder[Future],
                                     fetchBufferSize: Int = 1024 * 1024,
                                     ingestTimeout: Timeout = 120 seconds, 
                                     maxCacheSize: Int = 5,
                                     maxConsecutiveFailures: Int = 3) extends Actor {

  protected lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.KafkaShardIngestActor")

  private var lastCheckpoint: YggCheckpoint = initialCheckpoint

  private var totalConsecutiveFailures = 0
  private var ingestCache = TreeMap.empty[YggCheckpoint, Vector[EventMessage]] 
  private var pendingCompletes = Vector.empty[BatchComplete]

  implicit def M: Monad[Future]

  def receive = {
    case Status => sender ! status

    case complete @ BatchComplete(checkpoint, _) => 
      pendingCompletes :+= complete

      // the minimum value in the ingest cache is complete, so
      // all pending checkpoints from batches earlier than the
      // specified checkpoint can be flushed
      //logger.debug("BatchComplete insert. Head = %s, completed = %s".format(ingestCache.head, checkpoint))
      if (ingestCache.head._1 == checkpoint) {
        // reset failures count here since this state means that we've made forward progress
        totalConsecutiveFailures = 0

        pendingCompletes = pendingCompletes flatMap {
          // FIXME: Carefully review this logic; previously, the comparison here was being done using "<="
          // by the serialized JSON representation of the vector clocks (a silent side effect of
          // the silent implicit conversions to JValue in BlueEyes)
          case BatchComplete(pendingCheckpoint @ YggCheckpoint(_, clock), updated) if clock isDominatedBy checkpoint.messageClock =>
            handleBatchComplete(pendingCheckpoint, updated)
            None

          case stillPending => 
            Some(stillPending)
        }
      } 

      ingestCache -= checkpoint

    case BatchFailed(requestor, checkpoint) => 
      logger.warn("Incomplete ingest at " + checkpoint)
      totalConsecutiveFailures += 1
      if (totalConsecutiveFailures < maxConsecutiveFailures) {
        logger.info("Retrying failed ingest")
        for (messages <- ingestCache.get(checkpoint)) {
          val batchHandler = context.actorOf(Props(new BatchHandler(self, requestor, checkpoint, ingestTimeout))) 
          requestor.tell(IngestData(messages), batchHandler)
        }
      } else {
        // Blow up in spectacular fashion.
        logger.error("Halting ingest due to excessive consecutive failures at Kafka offsets: " + ingestCache.keys.map(_.offset).mkString("[", ", ", "]"))
        logger.error("Metadata is consistent up to the lower bound:"  + ingestCache.head._1)
        self ! PoisonPill
      }

    case GetMessages(requestor) => 
      logger.trace("Responding to GetMessages starting from checkpoint: " + lastCheckpoint)
      if (ingestEnabled) {
        if (ingestCache.size < maxCacheSize) {
          readRemote(lastCheckpoint).foreach {
            case Success((messages, checkpoint)) =>
              if (messages.size > 0) {
                logger.debug("Sending " + messages.size + " events to batch ingest handler.")

                // update the cache
                lastCheckpoint = checkpoint
                ingestCache += (checkpoint -> messages)
  
                // create a handler for the batch, then reply to the sender with the message set
                // using that handler reference as the sender to which the ingest system will reply
                val batchHandler = context.actorOf(Props(new BatchHandler(self, sender, checkpoint, ingestTimeout))) 
                requestor.tell(IngestData(messages), batchHandler)
              } else {
                logger.trace("No new data found after checkpoint: " + checkpoint)
                requestor ! IngestData(Nil)
              }
  
            case Failure(error)    => 
              logger.error("Error(s) occurred retrieving data from Kafka: " + error.message)
              requestor ! IngestErrors(List("Error(s) retrieving data from Kafka: " + error.message))
          }
        } else {
          logger.warn("Concurrent ingest window full (%d). Cannot start new ingest batch".format(ingestCache.size))
          requestor ! IngestData(Nil)
        }
      } else {
        logger.warn("Ingest disabled, skipping Getmessages request")
      }
  }

  /**
   * This method will be called on each completed batch. Subclasses may perform additional work here.
   */
  protected def handleBatchComplete(pendingCheckpoint: YggCheckpoint, updates: Seq[(ProjectionDescriptor, Option[ColumnMetadata])]): Unit

  private def readRemote(fromCheckpoint: YggCheckpoint): Future[Validation[Error, (Vector[EventMessage], YggCheckpoint)]] = {
    // The shard ingest actor needs to compute the maximum offset, so it has
    // to traverse the full message set in process; to avoid traversing it
    // twice, we simply read the payload into event messages at this point.
    // We stop at the first archive message, either including it if it's the
    // initial message, or using all inserts up to that point
    @tailrec
    def buildBatch(
        input: List[(EventMessage, Long)],
        apiKeyMap: Map[APIKey, AccountId],
        batch: Vector[EventMessage],
        checkpoint: YggCheckpoint): (Vector[EventMessage], YggCheckpoint) = {

      input match {
        case Nil =>
          (batch, checkpoint)

        case (event @ IngestMessage(apiKey, _, ownerAccountId0, records, _), offset) :: tail => 
          val newCheckpoint = records.foldLeft(checkpoint) {
            // TODO: This nested pattern match indicates that checkpoints are too closely 
            // coupled to the representation of event IDs.
            case (acc, IngestRecord(EventId(pid, sid), _)) => acc.update(offset, pid, sid)
          }

          apiKeyMap.get(apiKey) match {
            case Some(accountId) =>
              //FIXME: Deserialization need to do this.
              //val em = event.copy(ownerAccountId = ownerAccountId0.getOrElse(accountId))
              buildBatch(tail, apiKeyMap, batch :+ event, newCheckpoint)

            case None =>
              // Non-existent account means the account must have been deleted, so we discard the 
              // event here. TODO: This authorization should probably be done downstream instead,
              // since the archive authorization is not being done here.
              buildBatch(tail, apiKeyMap, batch, newCheckpoint)
          }
        
        case (ar: ArchiveMessage, _) :: tail if batch.nonEmpty =>
          logger.debug("Batch stopping on receipt of ArchiveMessage at offset/id %d/%d".format(offset, ar.archiveId.uid))
          (batch, checkpoint)

        case (ar @ ArchiveMessage(EventId(pid, sid), _), offset) :: tail =>
          // TODO: Where is the authorization checking credentials for the archive done?
          logger.debug("Singleton batch of ArchiveMessage at offset/id %d/%d".format(offset, ar.archiveId.uid))
          (Vector(ar), checkpoint.update(offset, pid, sid))
      }
    }

    val read = M.point {
      val req = new FetchRequest(
        topic,
        partition = 0,
        offset = lastCheckpoint.offset,
        maxSize = fetchBufferSize
      )

      // read a fetch buffer worth of messages from kafka, deserializing each one
      // and recording the offset
      val eventMessages: List[Validation[Error, (EventMessage, Long)]] = consumer.fetch(req).toList.map { msgAndOffset =>
        EventMessageEncoding.read(msgAndOffset.message.payload) map { (_, msgAndOffset.offset) }
      }

      val batched: Validation[Error, Future[(Vector[EventMessage], YggCheckpoint)]] = 
        eventMessages.sequence[({ type λ[α] = Validation[Error, α] })#λ, (EventMessage, Long)] map { messageSet =>
          val apiKeys = messageSet collect { 
            case (IngestMessage(apiKey, _, _, _, _), _) => apiKey 
          }

          accountFinder.mapAccountIds(apiKeys.toSet) map { apiKeyMap =>
            buildBatch(messageSet, apiKeyMap, Vector.empty, fromCheckpoint)
          }
        }

      batched.sequence[Future, (Vector[EventMessage], YggCheckpoint)]
    }

    read.join
  }

  protected def status: JValue = 
    JObject(JField("Ingest", JObject(JField("lastCheckpoint", lastCheckpoint.serialize) :: Nil)) :: Nil)

  override def postStop() = consumer.close
}

