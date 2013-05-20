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
import akka.util.Duration

import com.weiglewilczek.slf4s._
import org.slf4j._

import java.io._
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicInteger

import org.joda.time.Instant

import _root_.kafka.api.FetchRequest
import _root_.kafka.consumer.SimpleConsumer
import _root_.kafka.message.MessageSet

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz.{NonEmptyList => NEL, _}
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import Validation._

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import scalaz._
import scalaz.Validation._
import scalaz.effect._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.monoid._

//////////////
// MESSAGES //
//////////////

case class GetMessages(sendTo: ActorRef)

sealed trait ShardIngestAction
sealed trait IngestResult extends ShardIngestAction
case class IngestErrors(errors: Seq[String]) extends IngestResult
case class IngestData(messages: Seq[(Long, EventMessage)]) extends IngestResult

case class ProjectionUpdatesExpected(projections: Int)

////////////
// ACTORS //
////////////

trait IngestFailureLog {
  def logFailed(offset: Long, message: EventMessage, lastKnownGood: YggCheckpoint): IngestFailureLog
  def checkFailed(message: EventMessage): Boolean
  def restoreFrom: YggCheckpoint
  def persist: IO[IngestFailureLog]
}

case class FilesystemIngestFailureLog(failureLog: Map[EventMessage, FilesystemIngestFailureLog.LogRecord], restoreFrom: YggCheckpoint, persistDir: File) extends IngestFailureLog {
  import scala.math.Ordering.Implicits._
  import FilesystemIngestFailureLog._
  import FilesystemIngestFailureLog.LogRecord._

  def logFailed(offset: Long, message: EventMessage, lastKnownGood: YggCheckpoint): IngestFailureLog = {
    copy(failureLog = failureLog + (message -> LogRecord(offset, message, lastKnownGood)), restoreFrom = lastKnownGood min restoreFrom)
  }

  def checkFailed(message: EventMessage): Boolean = failureLog.contains(message)

  def persist: IO[IngestFailureLog] = IO {
    val logFile = new File(persistDir, FilePrefix + System.currentTimeMillis + ".tmp")
    val out = new PrintWriter(new FileWriter(logFile))
    try {
      for (rec <- failureLog.values) out.println(rec.serialize.renderCompact)
    } finally {
      out.close()
    }

    this
  }
}

object FilesystemIngestFailureLog {
  val FilePrefix = "ingest_failure_log-"
  def apply(persistDir: File, initialCheckpoint: YggCheckpoint): FilesystemIngestFailureLog = {
    persistDir.mkdirs()
    if (!persistDir.isDirectory) {
      throw new IllegalArgumentException(persistDir + " is not a directory usable for failure logs!")
    }

    def readAll(reader: BufferedReader, into: Map[EventMessage, LogRecord]): Map[EventMessage, LogRecord] = {
      val line = reader.readLine()
      if (line == null) into else {
        val logRecord = ((Thrown(_:Throwable)) <-: JParser.parseFromString(line)).flatMap(_.validated[LogRecord]).valueOr(err => sys.error(err.message))

        readAll(reader, into + (logRecord.message -> logRecord))
      }
    }

    val logFiles = persistDir.listFiles.toSeq.filter(_.getName.startsWith(FilePrefix))
    if (logFiles.isEmpty) new FilesystemIngestFailureLog(Map(), initialCheckpoint, persistDir)
    else {
      val reader = new BufferedReader(new FileReader(logFiles.maxBy(_.getName.substring(FilePrefix.length).dropRight(4).toLong)))
      try {
        val failureLog = readAll(reader, Map.empty[EventMessage, LogRecord])
        new FilesystemIngestFailureLog(failureLog, if (failureLog.isEmpty) initialCheckpoint else failureLog.values.minBy(_.lastKnownGood).lastKnownGood, persistDir)
      } finally {
        reader.close()
      }
    }
  }

  case class LogRecord(offset: Long, message: EventMessage, lastKnownGood: YggCheckpoint)
  object LogRecord {
    implicit val decomposer: Decomposer[LogRecord] = new Decomposer[LogRecord] {
      def decompose(rec: LogRecord) = JObject(
        "offset" -> rec.offset.serialize,
        "messageType" -> rec.message.fold(_ => "ingest", _ => "archive", _ => "storeFile").serialize,
        "message" -> rec.message.serialize,
        "lastKnownGood" -> rec.lastKnownGood.serialize
      )
    }

    implicit val extractor: Extractor[LogRecord] = new Extractor[LogRecord] {
      def validated(jv: JValue) = {
        for {
          offset  <- jv.validated[Long]("offset")
          msgType <- jv.validated[String]("messageType")
          message <- msgType match {
                       case "ingest" => jv.validated[EventMessage.EventMessageExtraction]("message")(IngestMessage.Extractor).flatMap {
                         _.map(Success(_)).getOrElse(Failure(Invalid("Incomplete ingest message")))
                       }

                       case "archive" => jv.validated[ArchiveMessage]("message")
                     }
          checkpoint <- jv.validated[YggCheckpoint]("lastKnownGood")
        } yield LogRecord(offset, message, checkpoint)
      }
    }
  }
}

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
                                     permissionsFinder: PermissionsFinder[Future],
                                     routingActor: ActorRef,
                                     ingestFailureLog: IngestFailureLog,
                                     fetchBufferSize: Int = 1024 * 1024,
                                     idleDelay: Duration = 1 seconds,
                                     ingestTimeout: Timeout = 120 seconds,
                                     maxCacheSize: Int = 5,
                                     maxConsecutiveFailures: Int = 3) extends Actor {

  protected lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.KafkaShardIngestActor")

  private var lastCheckpoint: YggCheckpoint = initialCheckpoint

  private var failureLog = ingestFailureLog
  private var totalConsecutiveFailures = 0
  private var ingestCache = TreeMap.empty[YggCheckpoint, Vector[(Long, EventMessage)]]
  private val runningBatches = new AtomicInteger
  private var pendingCompletes = Vector.empty[BatchComplete]
  private var initiated = 0

  implicit def M: Monad[Future]

  override def preStart() = {
    logger.info("Starting KafkaShardIngestActor")
    context.system.scheduler.schedule(idleDelay, idleDelay) {
      initiated += 1
      self ! GetMessages(routingActor)
    }
    logger.info("Recurring ingest request scheduled")
  }

  def receive = {
    case Status => sender ! status

    case complete @ BatchComplete(requestor, checkpoint) =>
      pendingCompletes :+= complete

      // the minimum value in the ingest cache is complete, so
      // all pending checkpoints from batches earlier than the
      // specified checkpoint can be flushed
      logger.debug("BatchComplete insert. Head = %s, completed = %s".format(ingestCache.headOption.map(_._1), checkpoint))
      if (ingestCache.headOption.exists(_._1 == checkpoint)) {
        // reset failures count here since this state means that we've made forward progress
        totalConsecutiveFailures = 0

        pendingCompletes = pendingCompletes flatMap {
          // FIXME: Carefully review this logic; previously, the comparison here was being done using "<="
          // by the serialized JSON representation of the vector clocks (a silent side effect of
          // the silent implicit conversions to JValue in BlueEyes)
          case BatchComplete(_, pendingCheckpoint @ YggCheckpoint(_, clock)) if clock isDominatedBy checkpoint.messageClock =>
            handleBatchComplete(pendingCheckpoint)
            None

          case stillPending =>
            Some(stillPending)
        }
      }

      ingestCache -= checkpoint
      runningBatches.getAndDecrement

      // Send off a new batch now that we've completed this one
      logger.trace("Pre-firing new GetMessages on batch completion to " + requestor)
      self.tell(GetMessages(requestor), requestor)

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
        //logger.error("Halting ingest due to excessive consecutive failures at Kafka offsets: " + ingestCache.keys.map(_.offset).mkString("[", ", ", "]"))
        logger.error("Skipping ingest batch due to excessive consecutive failures at Kafka offsets: " + ingestCache.keys.map(_.offset).mkString("[", ", ", "]"))
        logger.error("Metadata is consistent up to the lower bound:"  + ingestCache.head._1)
        logger.error("Ingest will continue, but query results may be inconsistent until the problem is resolved.")
        for (messages <- ingestCache.get(checkpoint).toSeq; (offset, ingestMessage) <- messages) {
          failureLog = failureLog.logFailed(offset, ingestMessage, lastCheckpoint)
        }

        runningBatches.getAndDecrement

        // Blow up in spectacular fashion.
        //self ! PoisonPill
      }

    case GetMessages(requestor) => try {
      logger.trace("Responding to GetMessages from %s starting from checkpoint %s. Running batches = %d/%d".format(requestor, lastCheckpoint, runningBatches.get, maxCacheSize))
        if (runningBatches.get < maxCacheSize) {
          // Funky handling of current count due to the fact that any errors will occur within a future
          runningBatches.getAndIncrement
          readRemote(lastCheckpoint).onSuccess {
            case Success((messages, checkpoint)) =>
              if (messages.size > 0) {
                logger.debug("Sending " + messages.size + " events to batch ingest handler.")

                // update the cache
                lastCheckpoint = checkpoint
                ingestCache += (checkpoint -> messages)

                // create a handler for the batch, then reply to the sender with the message set
                // using that handler reference as the sender to which the ingest system will reply
                val batchHandler = context.actorOf(Props(new BatchHandler(self, requestor, checkpoint, ingestTimeout)))
                batchHandler.tell(ProjectionUpdatesExpected(messages.size))
                requestor.tell(IngestData(messages), batchHandler)
              } else {
                logger.trace("No new data found after checkpoint: " + checkpoint)
                runningBatches.getAndDecrement
                requestor ! IngestData(Nil)
              }

            case Failure(error)    =>
              logger.error("Error(s) occurred retrieving data from Kafka: " + error.message)
              runningBatches.getAndDecrement
              requestor ! IngestErrors(List("Error(s) retrieving data from Kafka: " + error.message))
          }.onFailure {
            case t: Throwable =>
              runningBatches.getAndDecrement
              logger.error("Failure during remote message read", t); requestor ! IngestData(Nil)
          }
        } else {

          logger.warn("Concurrent ingest window full (%d). Cannot start new ingest batch".format(runningBatches.get))
          requestor ! IngestData(Nil)
        }
    } catch {
      case t: Throwable =>
        logger.error("Exception caught during ingest poll", t)
        requestor ! IngestErrors(List("Exception during poll: " + t.getMessage))
      }
  }

  /**
   * This method will be called on each completed batch. Subclasses may perform additional work here.
   */
  protected def handleBatchComplete(pendingCheckpoint: YggCheckpoint): Unit

  private def readRemote(fromCheckpoint: YggCheckpoint): Future[Validation[Error, (Vector[(Long, EventMessage)], YggCheckpoint)]] = {
    // The shard ingest actor needs to compute the maximum offset, so it has
    // to traverse the full message set in process; to avoid traversing it
    // twice, we simply read the payload into event messages at this point.
    // We stop at the first archive message, either including it if it's the
    // initial message, or using all inserts up to that point
    @tailrec
    def buildBatch(
        input: List[(Long, EventMessage)],
        batch: Vector[(Long, EventMessage)],
        checkpoint: YggCheckpoint): (Vector[(Long, EventMessage)], YggCheckpoint) = {

      input match {
        case Nil =>
          (batch, checkpoint)

        case (offset, event @ IngestMessage(_, _, _, records, _, _, _)) :: tail =>
          val newCheckpoint = if (records.isEmpty) {
            checkpoint.skipTo(offset)
          } else {
            records.foldLeft(checkpoint) {
              // TODO: This nested pattern match indicates that checkpoints are too closely
              // coupled to the representation of event IDs.
              case (acc, IngestRecord(EventId(pid, sid), _)) => acc.update(offset, pid, sid)
            }
          }

          buildBatch(tail, batch :+ (offset, event), newCheckpoint)

        case (offset, store : StoreFileMessage) :: tail =>
          val EventId(pid, sid) = store.eventId
          buildBatch(tail, batch :+ (offset, store), checkpoint.update(offset, pid, sid))

        case (offset, ArchiveMessage(_, _, _, eventId, _)) :: tail if batch.nonEmpty =>
          logger.debug("Batch stopping on receipt of ArchiveMessage at offset/id %d/%d".format(offset, eventId.uid))
          (batch, checkpoint)

        case (offset, ar @ ArchiveMessage(_, _, _, EventId(pid, sid), _)) :: tail =>
          // TODO: Where is the authorization checking credentials for the archive done?
          logger.debug("Singleton batch of ArchiveMessage at offset/id %d/%d".format(offset, ar.eventId.uid))
          if (failureLog.checkFailed(ar)) {
            // skip the message and continue without deletion.
            // FIXME: This is very dangerous; once we see these errors, we'll have to do a full reingest
            // from before the start of the error range.
            buildBatch(tail, batch, checkpoint.update(offset, pid, sid))
          } else {
            // return the archive as a standalone message
            (Vector(offset -> ar), checkpoint.update(offset, pid, sid))
          }
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

      val rawMessages = msTime({ t => logger.debug("Kafka fetch from %s:%d in %d ms".format(topic, lastCheckpoint.offset, t))}) {
        consumer.fetch(req)
      }

      val eventMessages: List[Validation[Error, (Long, EventMessage.EventMessageExtraction)]] =
        msTime({t => logger.debug("Raw kafka deserialization of %d events in %d ms".format(rawMessages.size, t)) }) {
          rawMessages.par.map { msgAndOffset =>
            EventMessageEncoding.read(msgAndOffset.message.payload) map { (msgAndOffset.offset, _) }
          }.toList
        }

      val batched: Validation[Error, Future[(Vector[(Long, EventMessage)], YggCheckpoint)]] =
        eventMessages.sequence[({ type λ[α] = Validation[Error, α] })#λ, (Long, EventMessage.EventMessageExtraction)] map { messageSet =>
          val apiKeys: List[(APIKey, Path)] = messageSet collect {
            case (_, \/-(IngestMessage(apiKey, path, _, _, _, _, _))) => (apiKey, path)
            case (_, -\/((apiKey, path, _))) => (apiKey, path)
          }

          val authorityCacheFutures = apiKeys.distinct map {
            case k @ (apiKey, path) =>
              // infer write authorities without a timestamp here, because we'll only use this for legacy events
              permissionsFinder.inferWriteAuthorities(apiKey, path, None) map { k -> _ }
          }

          authorityCacheFutures.sequence map { cached =>
            val authorityCache = cached.foldLeft(Map.empty[(APIKey, Path), Authorities]) {
              case (acc, (k, Some(a))) => acc + (k -> a)
              case (acc, (_, None)) => acc
            }

            val updatedMessages: List[(Long, EventMessage)] = messageSet.flatMap {
              case (offset, \/-(message)) =>
                Some((offset, message))

              case (offset, -\/((apiKey, path, genMessage))) =>
                authorityCache.get((apiKey, path)) map { authorities =>
                  Some((offset, genMessage(authorities)))
                } getOrElse {
                  logger.warn("Discarding event at offset %d with apiKey %s for path %s because we could not determine the account".format(offset, apiKey, path))
                  None
                }
            }

            buildBatch(updatedMessages, Vector.empty, fromCheckpoint)
          }
        }

      batched.sequence[Future, (Vector[(Long, EventMessage)], YggCheckpoint)]
    }

    read.join
  }

  protected def status: JValue =
    JObject(JField("Ingest", JObject(JField("lastCheckpoint", lastCheckpoint.serialize) :: Nil)) :: Nil)

  override def postStop() = {
    consumer.close
    failureLog.persist.unsafePerformIO
  }
}
