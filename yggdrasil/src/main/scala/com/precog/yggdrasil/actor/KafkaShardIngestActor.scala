package com.precog.yggdrasil
package actor

import metadata.ColumnMetadata
import com.precog.util._
import com.precog.common._
import com.precog.common.kafka._
import ColumnMetadata.monoid

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.PoisonPill
import akka.util.duration._
import akka.util.Timeout

import com.weiglewilczek.slf4s._

import _root_.kafka.api.FetchRequest
import _root_.kafka.consumer.SimpleConsumer
import _root_.kafka.message.MessageSet

import blueeyes.json.JsonAST._
import blueeyes.json.xschema.Decomposer
import blueeyes.json.xschema.DefaultSerialization._

import scala.collection.mutable
import scala.collection.immutable.TreeMap
import scalaz._
import scalaz.syntax.monoid._

//////////////
// MESSAGES //
//////////////

case class ProjectionInsertsExpected(projections: Int)

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
class KafkaShardIngestActor(initialCheckpoint: YggCheckpoint, metadataActor: ActorRef, consumer: SimpleConsumer, topic: String, 
                            fetchBufferSize: Int = 1024 * 1024, ingestTimeout: Timeout = 30 seconds, 
                            maxCacheSize: Int = 5, maxConsecutiveFailures: Int = 3) extends Actor with Logging {

  import KafkaBatchHandler._

  private var lastCheckpoint = initialCheckpoint

  private var totalConsecutiveFailures = 0
  private var ingestCache = TreeMap.empty[YggCheckpoint, Vector[EventMessage]] 
  private var pendingCompletes = Vector.empty[Complete]

  def receive = {
    case Status => sender ! status

    case complete @ Complete(checkpoint, projectionMetadata) => 
      pendingCompletes :+= complete

      // the minimum value in the ingest cache is complete, so
      // all pending checkpoints from batches earlier than the
      // specified checkpoint can be flushed
      if (ingestCache.head == checkpoint) {
        // reset failures count here since this state means that we've made forward progress
        totalConsecutiveFailures = 0

        pendingCompletes = pendingCompletes flatMap {
          case Complete(pendingCheckpoint, metadata) if pendingCheckpoint <= checkpoint =>
            metadataActor ! IngestBatchMetadata(metadata, pendingCheckpoint)    
            None

          case stillPending => 
            Some(stillPending)
        }
      } 

      ingestCache -= checkpoint

    case Incomplete(requestor, checkpoint) => 
      totalConsecutiveFailures += 1
      if (totalConsecutiveFailures < maxConsecutiveFailures) {
        for (messages <- ingestCache.get(checkpoint)) {
          val batchHandler = context.actorOf(Props(new KafkaBatchHandler(self, requestor, checkpoint, ingestTimeout))) 
          requestor.tell(IngestData(messages), batchHandler)
        }
      } else {
        // Blow up in spectacular fashion.
        logger.error("Halting ingest due to excessive consecutive failures at Kafka offsets: " + ingestCache.keys.map(_.offset).mkString("[", ", ", "]"))
        logger.error("Metadata is consistent up to the lower bound:"  + ingestCache.head)
        self ! PoisonPill
      }

    case GetMessages => 
      if (ingestCache.size < maxCacheSize) {
        readRemote(lastCheckpoint) match {
          case Success((messages, checkpoint)) => 
            // update the cache
            lastCheckpoint = checkpoint
            ingestCache += (checkpoint -> messages)

            // create a handler for the batch, then reply to the sender with the message set
            // using that handler reference as the sender to which the ingest system will reply
            val batchHandler = context.actorOf(Props(new KafkaBatchHandler(self, sender, checkpoint, ingestTimeout))) 
            sender.tell(IngestData(messages), batchHandler)

          case Failure(error)    => 
            logger.error("An error occurred retrieving data from Kafka.", error)
            sender ! IngestErrors(List("An error occurred retrieving data from Kafka: " + error.getMessage))
        }
      } else {
        IngestData(Nil)
      }
  }

  private def readRemote(fromCheckpoint: YggCheckpoint): Validation[Throwable, (Vector[EventMessage], YggCheckpoint)] = {
    Validation.fromTryCatch {
      val messageSet = consumer.fetch(new FetchRequest(topic, partition = 0, offset = lastCheckpoint.offset, maxSize = fetchBufferSize))

      // The shard ingest actor needs to compute the maximum offset, so it has to traverse the full 
      // message set in process; to avoid traversing it twice, we simply read the payload into 
      // event messages at this point.
      messageSet.foldLeft((Vector.empty[EventMessage], fromCheckpoint)) {
        case ((acc, YggCheckpoint(offset, clock)), msgAndOffset) => 
          IngestMessageSerialization.read(msgAndOffset.message.payload) match {
            case em @ EventMessage(EventId(pid, sid), _) =>
              (acc :+ em, YggCheckpoint(offset max msgAndOffset.offset, clock.update(pid, sid)))
          }
      }
    }
  }

  protected def status: JValue = 
    JObject(JField("Ingest", JObject(JField("lastCheckpoint", lastCheckpoint.serialize) :: Nil)) :: Nil)

  override def postStop() = consumer.close
}

/**
 * A batch handler actor is responsible for tracking confirmation of persistence for
 * all the messages in a specific batch. It sends 
 */
class KafkaBatchHandler(ingestActor: ActorRef, requestor: ActorRef, checkpoint: YggCheckpoint, ingestTimeout: Timeout) extends Actor {
  import KafkaBatchHandler._

  private var remaining = -1 
  private var projectionMetadata = Map.empty[ProjectionDescriptor, ColumnMetadata]

  override def preStart() = {
    context.system.scheduler.scheduleOnce(ingestTimeout.duration, self, PoisonPill)
  }

  def receive = {
    case ProjectionInsertsExpected(count) => 
      remaining += (count + 1)
      if (remaining == 0) self ! PoisonPill

    case InsertMetadata(descriptor, columnMetadata) =>
      projectionMetadata += (descriptor -> (projectionMetadata.getOrElse(descriptor, ColumnMetadata.Empty) |+| columnMetadata))
      remaining -= 1
      if (remaining == 0) self ! PoisonPill
  }

  override def postStop() = {
    // if the ingest isn't complete by the timeout, ask the requestor to retry
    if (remaining != 0) {
      ingestActor ! Incomplete(requestor, checkpoint)
    } else {
      // update the metadatabase
      ingestActor ! Complete(checkpoint, projectionMetadata)
    }
  }
}

object KafkaBatchHandler {
  case class Complete(checkpoint: YggCheckpoint, projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata])
  case class Incomplete(requestor: ActorRef, checkpoint: YggCheckpoint)
}

