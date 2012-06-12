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
import akka.dispatch.Await
import akka.pattern.ask
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
class KafkaShardIngestActor(shardId: String, systemCoordination: SystemCoordination, metadataActor: ActorRef, consumer: SimpleConsumer, topic: String, ingestEnabled: Boolean,
                            fetchBufferSize: Int = 1024 * 1024, ingestTimeout: Timeout = 120 seconds, 
                            maxCacheSize: Int = 5, maxConsecutiveFailures: Int = 3) extends Actor with Logging {

  import KafkaBatchHandler._

  private var lastCheckpoint: YggCheckpoint = _

  private var totalConsecutiveFailures = 0
  private var ingestCache = TreeMap.empty[YggCheckpoint, Vector[EventMessage]] 
  private var pendingCompletes = Vector.empty[Complete]

  override def preStart(): Unit = {
    implicit val timeout = Timeout(45000l)
    // We can't have both actors trying to lock the ZK element or we race, so we just delegate to the metadataActor
    logger.debug("Requesting checkpoint from metadata")
    lastCheckpoint = Await.result(metadataActor ? GetCurrentCheckpoint, 45 seconds).asInstanceOf[YggCheckpoint]
    logger.debug("Checkpoint load complete")
  }

  def receive = {
    case Status => sender ! status

    case complete @ Complete(checkpoint, projectionMetadata) => 
      pendingCompletes :+= complete

      // the minimum value in the ingest cache is complete, so
      // all pending checkpoints from batches earlier than the
      // specified checkpoint can be flushed
      //logger.debug("Complete insert. Head = %s, completed = %s".format(ingestCache.head, checkpoint))
      if (ingestCache.head._1 == checkpoint) {
        // reset failures count here since this state means that we've made forward progress
        totalConsecutiveFailures = 0

        pendingCompletes = pendingCompletes flatMap {
          case Complete(pendingCheckpoint, metadata) if pendingCheckpoint <= checkpoint =>
            logger.debug(pendingCheckpoint + " to be updated")
            metadataActor ! IngestBatchMetadata(metadata, pendingCheckpoint.messageClock, Some(pendingCheckpoint.offset))
            None

          case stillPending => 
            Some(stillPending)
        }
      } 

      ingestCache -= checkpoint

    case Incomplete(requestor, checkpoint) => 
      logger.warn("Incomplete ingest at " + checkpoint)
      totalConsecutiveFailures += 1
      if (totalConsecutiveFailures < maxConsecutiveFailures) {
        for (messages <- ingestCache.get(checkpoint)) {
          val batchHandler = context.actorOf(Props(new BatchHandler(self, requestor, checkpoint, ingestTimeout))) 
          requestor.tell(IngestData(messages), batchHandler)
        }
      } else {
        // Blow up in spectacular fashion.
        logger.error("Halting ingest due to excessive consecutive failures at Kafka offsets: " + ingestCache.keys.map(_.offset).mkString("[", ", ", "]"))
        logger.error("Metadata is consistent up to the lower bound:"  + ingestCache.head)
        self ! PoisonPill
      }

    case GetMessages(requestor) => 
      if (ingestEnabled) {
        if (ingestCache.size < maxCacheSize) {
          readRemote(lastCheckpoint) match {
            case Success((messages, checkpoint)) => 
              if (messages.size > 0) {
                // update the cache
                lastCheckpoint = checkpoint
                ingestCache += (checkpoint -> messages)
  
                // create a handler for the batch, then reply to the sender with the message set
                // using that handler reference as the sender to which the ingest system will reply
                val batchHandler = context.actorOf(Props(new BatchHandler(self, sender, checkpoint, ingestTimeout))) 
                requestor.tell(IngestData(messages), batchHandler)
              } else {
                requestor ! IngestData(Nil)
              }
  
            case Failure(error)    => 
              logger.error("An error occurred retrieving data from Kafka.", error)
              requestor ! IngestErrors(List("An error occurred retrieving data from Kafka: " + error.getMessage))
          }
        } else {
          logger.debug("ingestCache.size too big (%d)".format(ingestCache.size))
          requestor ! IngestData(Nil)
        }
      } else {
        logger.warn("Ingest disabled, skipping Getmessages request")
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
class BatchHandler(ingestActor: ActorRef, requestor: ActorRef, checkpoint: YggCheckpoint, ingestTimeout: Timeout) extends Actor with Logging {
  import KafkaBatchHandler._

  private var remaining = -1 
  private var projectionMetadata = Map.empty[ProjectionDescriptor, ColumnMetadata]

  override def preStart() = {
    context.system.scheduler.scheduleOnce(ingestTimeout.duration, self, PoisonPill)
  }

  def receive = {
    case ProjectionInsertsExpected(count) => 
      remaining += (count + 1)
      logger.debug("Should expect %d more inserts (total %d)".format(count, remaining))
      if (remaining == 0) self ! PoisonPill

    case InsertMetadata(descriptor, columnMetadata) =>
      logger.debug("Insert meta complete for " + descriptor)
      projectionMetadata += (descriptor -> (projectionMetadata.getOrElse(descriptor, ColumnMetadata.Empty) |+| columnMetadata))
      remaining -= 1
      if (remaining == 0) self ! PoisonPill
  }

  override def postStop() = {
    // if the ingest isn't complete by the timeout, ask the requestor to retry
    if (remaining != 0) {
      logger.info("Incomplete with %d remaining".format(remaining))
      ingestActor ! Incomplete(requestor, checkpoint)
    } else {
      // update the metadatabase, by way of notifying the ingest actor
      // so that any pending completions that arrived out of order can be cleared.
      logger.info("Sending complete on batch")
      ingestActor ! Complete(checkpoint, projectionMetadata)
    }
  }
}

object KafkaBatchHandler {
  case class Complete(checkpoint: YggCheckpoint, projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata])
  case class Incomplete(requestor: ActorRef, checkpoint: YggCheckpoint)
}

// Kinda hacky, just get an actor that we can use to wait for batch completion outside of the actor system
case object PollBatch

class PollBatchActor extends Actor {
  private var result: Option[Any] = None
  def receive = {
    case PollBatch => sender ! result

    case other     => result = Some(other)
  }
}
