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
import com.precog.accounts.BasicAccountManager
import com.precog.util._
import com.precog.common._
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
import blueeyes.json.serialization.Decomposer
import blueeyes.json.serialization.DefaultSerialization._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.immutable.TreeMap
import scalaz._
import scalaz.syntax.monoid._

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
                                     accountManager: BasicAccountManager[Future],
                                     fetchBufferSize: Int = 1024 * 1024,
                                     ingestTimeout: Timeout = 120 seconds, 
                                     maxCacheSize: Int = 5,
                                     maxConsecutiveFailures: Int = 3) extends Actor {
  protected lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.KafkaShardIngestActor")

  private var lastCheckpoint: YggCheckpoint = initialCheckpoint

  private var totalConsecutiveFailures = 0
  private var ingestCache = TreeMap.empty[YggCheckpoint, Vector[IngestMessage]] 
  private var pendingCompletes = Vector.empty[BatchComplete]

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
          case BatchComplete(pendingCheckpoint, updated) if pendingCheckpoint <= checkpoint =>
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
              logger.error("An error occurred retrieving data from Kafka.", error)
              requestor ! IngestErrors(List("An error occurred retrieving data from Kafka: " + error.getMessage))
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

  private def readRemote(fromCheckpoint: YggCheckpoint):
    Future[Validation[Throwable, (Vector[IngestMessage], YggCheckpoint)]] = {

    // The shard ingest actor needs to compute the maximum offset, so it has
    // to traverse the full message set in process; to avoid traversing it
    // twice, we simply read the payload into event messages at this point.
    // We stop at the first archive message, either including it if it's the
    // initial message, or using all inserts up to that point
    @tailrec
    def buildBatch(
      input: List[(IngestMessage,Long)],
      apiKeyMap: Map[APIKey, Set[AccountID]],
      batch: Vector[IngestMessage],
      checkpoint: YggCheckpoint
    ): (Vector[IngestMessage], YggCheckpoint) = input match {
      case Nil =>
        (batch, checkpoint)
      case (emOrig @ EventMessage(EventId(pid, sid), event), offset) :: tail => {
        val accountIds = apiKeyMap(event.apiKey)
        if (accountIds.size == 0) {
          // Non-existent account means it must be deleted, so we discard
          buildBatch(tail, apiKeyMap, batch, checkpoint.update(offset, pid, sid))
        } else {
          if (accountIds.size != 1) {
            throw new Exception("Invalid account ID results for apiKey %s : %s".format(accountIds, event.apiKey))
          } else {
            val em = emOrig.copy(event = event.copy(ownerAccountId = Some(accountIds.head)))
            buildBatch(tail, apiKeyMap, batch :+ em, checkpoint.update(offset, pid, sid))
          }
        }
      }
      case (ar: ArchiveMessage, _) :: tail if batch.nonEmpty =>
        (batch, checkpoint)
      case (ar @ ArchiveMessage(ArchiveId(pid, sid), _), offset) :: tail =>
        (Vector(ar), checkpoint.update(offset, pid, sid))
    }

    Validation.fromTryCatch {
      val req = new FetchRequest(
        topic,
        partition = 0,
        offset = lastCheckpoint.offset,
        maxSize = fetchBufferSize
      )

      consumer.fetch(req).toList.map {
        msgAndOffset =>
          (IngestMessageSerialization.read(msgAndOffset.message.payload), msgAndOffset.offset)
      }
    } match {
      case Success(messageSet) => {
        accountManager.mapAccountIds(messageSet.collect {
          case (EventMessage(_, event), _) => event.apiKey
        }.toSet).map { apiKeyMap =>
          Success(buildBatch(messageSet, apiKeyMap, Vector.empty, fromCheckpoint))
        }
      }
      case Failure(t) => {
        import context.system
        implicit val executor = ExecutionContext.defaultExecutionContext
        Future(Failure[Throwable, (Vector[IngestMessage], YggCheckpoint)](t))
      }
    }
  }

  protected def status: JValue = 
    JObject(JField("Ingest", JObject(JField("lastCheckpoint", lastCheckpoint.serialize) :: Nil)) :: Nil)

  override def postStop() = consumer.close
}

