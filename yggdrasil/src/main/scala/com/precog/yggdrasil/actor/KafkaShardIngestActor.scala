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

import java.io._
import java.util.concurrent.TimeUnit.SECONDS

import _root_.kafka.api.FetchRequest
import _root_.kafka.consumer.SimpleConsumer
import _root_.kafka.message.MessageSet

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.JavaConverters._
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

case class ProjectionUpdatesExpected(projections: Int)

////////////
// ACTORS //
////////////

trait IngestFailureLog {
  def logFailed(offset: Long, message: IngestMessage, lastKnownGood: YggCheckpoint): IngestFailureLog
  def checkFailed(eventId: EventId): Boolean
  def restoreFrom: YggCheckpoint
  def persist: IO[IngestFailureLog]
}

case class FilesystemIngestFailureLog(failureLog: Map[EventId, FilesystemIngestFailureLog.LogRecord], restoreFrom: YggCheckpoint, persistDir: File) extends IngestFailureLog {
  import scala.math.Ordering.Implicits._
  import FilesystemIngestFailureLog._
  import FilesystemIngestFailureLog.LogRecord._

  def logFailed(offset: Long, message: IngestMessage, lastKnownGood: YggCheckpoint): IngestFailureLog = {
    copy(failureLog = failureLog + (message.eventId -> LogRecord(offset, message, lastKnownGood)), restoreFrom = lastKnownGood min restoreFrom)
  }

  def checkFailed(eventId: EventId): Boolean = failureLog.contains(eventId)

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
    
    def readAll(reader: BufferedReader, into: Map[EventId, LogRecord]): Map[EventId, LogRecord] = {
      val line = reader.readLine()
      if (line == null) into else {
        val logRecord = ((Thrown(_:Throwable)) <-: JParser.parseFromString(line)).flatMap(_.validated[LogRecord]).valueOr(err => sys.error(err.message))
                        
        readAll(reader, into + (logRecord.message.eventId -> logRecord))
      }
    }

    val logFiles = persistDir.listFiles.toSeq.filter(_.getName.startsWith(FilePrefix))
    if (logFiles.isEmpty) new FilesystemIngestFailureLog(Map(), initialCheckpoint, persistDir)
    else {
      val reader = new BufferedReader(new FileReader(logFiles.maxBy(_.getName.substring(FilePrefix.length).toLong)))
      try {
        val failureLog = readAll(reader, Map.empty[EventId, LogRecord])
        new FilesystemIngestFailureLog(failureLog, if (failureLog.isEmpty) initialCheckpoint else failureLog.values.minBy(_.lastKnownGood).lastKnownGood, persistDir) 
      } finally {
        reader.close()
      }
    }
  }

  case class LogRecord(offset: Long, message: IngestMessage, lastKnownGood: YggCheckpoint)
  object LogRecord {
    implicit val decomposer: Decomposer[LogRecord] = new Decomposer[LogRecord] {
      def decompose(rec: LogRecord) = 
        JObject("offset" -> rec.offset.serialize, "message" -> rec.message.serialize, "lastKnownGood" -> rec.lastKnownGood.serialize)
    }

    implicit val extractor: Extractor[LogRecord] = new Extractor[LogRecord] with ValidatedExtraction[LogRecord] {
      override def validated(jv: JValue) = {
        ( (jv \ "offset").validated[Long] |@|
          (jv \ "message").validated[IngestMessage] |@|
          (jv \ "lastKnownGood").validated[YggCheckpoint] ) { LogRecord.apply _ }
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
                                     accountManager: BasicAccountManager[Future],
                                     ingestFailureLog: IngestFailureLog,
                                     fetchBufferSize: Int = 1024 * 1024,
                                     ingestTimeout: Timeout = 120 seconds, 
                                     maxCacheSize: Int = 5,
                                     maxConsecutiveFailures: Int = 3) extends Actor {
  protected lazy val logger = LoggerFactory.getLogger("com.precog.yggdrasil.actor.KafkaShardIngestActor")

  private var lastCheckpoint: YggCheckpoint = initialCheckpoint

  private var failureLog = ingestFailureLog
  private var totalConsecutiveFailures = 0
  private var ingestCache = TreeMap.empty[YggCheckpoint, Vector[(Long, IngestMessage)]] 
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
          requestor.tell(IngestData(messages.map(_._2)), batchHandler)
        }
      } else {
        //logger.error("Halting ingest due to excessive consecutive failures at Kafka offsets: " + ingestCache.keys.map(_.offset).mkString("[", ", ", "]"))
        logger.error("Skipping ingest batch due to excessive consecutive failures at Kafka offsets: " + ingestCache.keys.map(_.offset).mkString("[", ", ", "]"))
        logger.error("Metadata is consistent up to the lower bound:"  + ingestCache.head._1)
        logger.error("Ingest will continue, but query results may be inconsistent until the problem is resolved.")
        for (messages <- ingestCache.get(checkpoint).toSeq; (offset, ingestMessage) <- messages) {
          failureLog = failureLog.logFailed(offset, ingestMessage, lastCheckpoint)
        }

        // Blow up in spectacular fashion.
        //self ! PoisonPill
      }

    case GetMessages(requestor) => try {
      logger.trace("Responding to GetMessages starting from checkpoint: " + lastCheckpoint)
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
              requestor.tell(IngestData(messages.map(_._2)), batchHandler)
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
    } catch {
      case t: Throwable => 
        logger.error("Exception caught during ingest poll", t)
        requestor ! IngestErrors(List("Exception during poll: " + t.getMessage))
    }
  }

  /**
   * This method will be called on each completed batch. Subclasses may perform additional work here.
   */
  protected def handleBatchComplete(pendingCheckpoint: YggCheckpoint, updates: Seq[(ProjectionDescriptor, Option[ColumnMetadata])]): Unit

  private def readRemote(fromCheckpoint: YggCheckpoint):
    Future[Validation[Throwable, (Vector[(Long, IngestMessage)], YggCheckpoint)]] = {

    // The shard ingest actor needs to compute the maximum offset, so it has
    // to traverse the full message set in process; to avoid traversing it
    // twice, we simply read the payload into event messages at this point.
    // We stop at the first archive message, either including it if it's the
    // initial message, or using all inserts up to that point
    @tailrec
    def buildBatch(
      input: List[(Long, IngestMessage)],
      apiKeyMap: Map[APIKey, Set[AccountId]],
      batch: Vector[(Long, IngestMessage)],
      checkpoint: YggCheckpoint
    ): (Vector[(Long, IngestMessage)], YggCheckpoint) = input match {
      case Nil =>
        (batch, checkpoint)

      case (offset, emOrig @ EventMessage(eid @ EventId(pid, sid), event)) :: tail => 
        val accountIds = apiKeyMap(event.apiKey)
        if (accountIds.size == 0 || failureLog.checkFailed(eid)) {
          // Non-existent account or previously failed event Id means it must be discarded/skipped
          buildBatch(tail, apiKeyMap, batch, checkpoint.update(offset, pid, sid))
        } else {
          if (accountIds.size != 1) {
            throw new Exception("Invalid account ID results for apiKey %s : %s".format(accountIds, event.apiKey))
          } else {
            val em = if (event.ownerAccountId.isDefined) emOrig else emOrig.copy(event = event.copy(ownerAccountId = Some(accountIds.head)))
            buildBatch(tail, apiKeyMap, batch :+ (offset, em), checkpoint.update(offset, pid, sid))
          }
        }
      
      case (offset, ar: ArchiveMessage) :: tail if batch.nonEmpty =>
        logger.debug("Batch stopping on receipt of ArchiveMessage at offset/id %d/%d".format(offset, ar.archiveId.uid))
        (batch, checkpoint)

      case (offset, ar @ ArchiveMessage(ArchiveId(pid, sid), _)) :: tail =>
        logger.debug("Singleton batch of ArchiveMessage at offset/id %d/%d".format(offset, ar.archiveId.uid))
        if (failureLog.checkFailed(ar.eventId)) {
          // skip the message and continue without deletion.
          // FIXME: This is very dangerous; once we see these errors, we'll have to do a full reingest
          // from before the start of the error range.
          buildBatch(tail, apiKeyMap, batch, checkpoint.update(offset, pid, sid))
        } else {
          // return the archive as a standalone message
          (Vector(offset -> ar), checkpoint.update(offset, pid, sid))
        }
    }

    Validation.fromTryCatch {
      val req = new FetchRequest(
        topic,
        partition = 0,
        offset = lastCheckpoint.offset,
        maxSize = fetchBufferSize
      )

      consumer.fetch(req).toList.map { msgAndOffset =>
        (msgAndOffset.offset, IngestMessageSerialization.read(msgAndOffset.message.payload))
      }
    } match {
      case Success(messageSet) => 
        accountManager.mapAccountIds(messageSet.collect { case (_, EventMessage(_, event)) => event.apiKey }.toSet).map { apiKeyMap =>
          Success(buildBatch(messageSet, apiKeyMap, Vector.empty, fromCheckpoint))
        }
      
      case Failure(t) => 
        import context.system
        implicit val executor = ExecutionContext.defaultExecutionContext
        Future(Failure[Throwable, (Vector[(Long, IngestMessage)], YggCheckpoint)](t))
    }
  }

  protected def status: JValue = 
    JObject(JField("Ingest", JObject(JField("lastCheckpoint", lastCheckpoint.serialize) :: Nil)) :: Nil)

  override def postStop() = {
    consumer.close
    failureLog.persist.unsafePerformIO
  }
}

