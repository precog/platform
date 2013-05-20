package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.kafka._
import com.precog.common.security._
import com.precog.util.PrecogUnit

import blueeyes.bkka._
import blueeyes.json.serialization.Extractor.Error

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s._

import _root_.kafka.api._
import _root_.kafka.common._
import _root_.kafka.consumer._
import _root_.kafka.message._
import _root_.kafka.producer._

import java.util.Properties
import org.joda.time.Instant
import org.streum.configrity.{Configuration, JProperties}

import scalaz._
import scalaz.std.list._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._
import scala.annotation.tailrec

object KafkaRelayAgent extends Logging {
  def apply(permissionsFinder: PermissionsFinder[Future], eventIdSeq: EventIdSequence, localConfig: Configuration, centralConfig: Configuration)(implicit executor: ExecutionContext): (KafkaRelayAgent, Stoppable) = {

    val localTopic = localConfig[String]("topic", "local_event_cache")
    val centralTopic = centralConfig[String]("topic", "central_event_store")
    val maxMessageSize = centralConfig[Int]("broker.max_message_size", 1000000)

    val centralProperties: java.util.Properties = {
      val props = JProperties.configurationToProperties(centralConfig)
      props.setProperty("max.message.size", maxMessageSize.toString)
      props
    }

    val producer = new Producer[String, Message](new ProducerConfig(centralProperties))

    val consumerHost = localConfig[String]("broker.host", "localhost")
    val consumerPort = localConfig[String]("broker.port", "9082").toInt
    val consumer = new SimpleConsumer(consumerHost, consumerPort, 5000, 64 * 1024)

    val relayAgent = new KafkaRelayAgent(permissionsFinder, eventIdSeq, consumer, localTopic, producer, centralTopic, maxMessageSize)
    val stoppable = Stoppable.fromFuture(relayAgent.stop map { _ => consumer.close; producer.close })

    new Thread(relayAgent).start()
    (relayAgent, stoppable)
  }
}
/** An independent agent that will consume records using the specified consumer,
  * augment them with record identities, then send them with the specified producer. */
final class KafkaRelayAgent(
  permissionsFinder: PermissionsFinder[Future], eventIdSeq: EventIdSequence,
  consumer: SimpleConsumer, localTopic: String,
  producer: Producer[String, Message], centralTopic: String,
  maxMessageSize: Int,
  bufferSize: Int = 1024 * 1024, retryDelay: Long = 5000L,
  maxDelay: Double = 100.0, waitCountFactor: Int = 25)(implicit executor: ExecutionContext) extends Runnable with Logging {

  logger.info("Allocating KafkaRelayAgent, hash = " + hashCode)

  val centralCodec = new KafkaEventMessageCodec

  @volatile private var runnable = true;
  private val stopPromise = Promise[PrecogUnit]()
  private implicit val M: Monad[Future] = new FutureMonad(executor)

  def stop: Future[PrecogUnit] = Future({ runnable = false }) flatMap { _ => stopPromise }

  override def run() {
    if (runnable) {
      val offset = eventIdSeq.getLastOffset()
      logger.debug("Kafka consumer starting from offset: " + offset)
      ingestBatch(offset, 0, 0, 0)
    }
  }

  private def ingestBatch(offset: Long, batch: Long, delay: Long, waitCount: Long, retries: Int = 5): Unit = {
    if (runnable) {
      try {
        if(batch % 100 == 0) logger.debug("Processing kafka consumer batch %d [%s]".format(batch, if(waitCount > 0) "IDLE" else "ACTIVE"))
        val fetchRequest = new FetchRequest(localTopic, 0, offset, bufferSize)

        val messages = consumer.fetch(fetchRequest) // try/catch is for this line. Okay to wrap in a future & flatMap instead?
        forwardAll(messages.toList) onSuccess {
          case _ =>
            val newDelay = delayStrategy(messages.sizeInBytes.toInt, delay, waitCount)

            val (newOffset, newWaitCount) = if(messages.size > 0) {
              val o: Long = messages.last.offset
              logger.debug("Kafka consumer batch size: %d offset: %d)".format(messages.size, o))
              (o, 0L)
            } else {
              (offset, waitCount + 1)
            }

            Thread.sleep(newDelay)

            ingestBatch(newOffset, batch + 1, newDelay, newWaitCount)
        } onFailure {
          case error =>
            logger.error("Batch ingest failed at offset %d batch %d; retrying. Data transfer to central queue halted pending manual intervention.".format(offset, batch), error)
            runnable = false
            stopPromise.success(PrecogUnit)
        }
      } catch {
        case ex =>
          if (retries > 0) {
            logger.error("An unexpected error occurred retrieving messages from local kafka consumer. Retrying from offset %d batch %d.".format(offset, batch), ex)
            ingestBatch(offset, batch, delay, waitCount, retries - 1)
          } else {
            logger.error("An unexpected error occurred retrieving messages from local kafka consumer. Halting at offset %d batch %d.".format(offset, batch), ex)
            runnable = false
            stopPromise.success(PrecogUnit)
          }
      }
    } else {
      logger.info("Kafka relay agent shutdown request detected. Halting at offset %d batch %d.".format(offset, batch))
      stopPromise.success(PrecogUnit)
    }
  }

  private def delayStrategy(messageBytes: Int, currentDelay: Long, waitCount: Long): Long = {
    if(messageBytes == 0) {
      val boundedWaitCount = if (waitCount > waitCountFactor) waitCountFactor else waitCount
      (maxDelay * boundedWaitCount / waitCountFactor).toLong
    } else {
      (maxDelay * (1.0 - messageBytes.toDouble / bufferSize)).toLong
    }
  }

  private case class Authorized(event: Event, offset: Long, authorities: Option[Authorities])

  private def forwardAll(messages: List[MessageAndOffset]) = {
    val outgoing: List[Validation[Error, Future[Authorized]]] = messages map { msg =>
      EventEncoding.read(msg.message.payload) map { ev => deriveAuthority(ev).map { Authorized(ev, msg.offset, _) } }
    }

    outgoing.sequence[({ type λ[α] = Validation[Error, α] })#λ, Future[Authorized]] map { messageFutures =>
      Future.sequence(messageFutures) map { messages: List[Authorized] =>
        val identified: List[Message] = messages.flatMap {
          case Authorized(Ingest(apiKey, path, _, data, jobId, timestamp, storeMode), offset, Some(authorities)) =>
            def encodeIngestMessages(ev: List[IngestMessage]): List[Message] = {
              val messages = ev.map(centralCodec.toMessage)

              if (messages.forall(_.size <= maxMessageSize)) {
                messages
              } else {
                if (ev.size == data.length) {
                  logger.error("Failed to reach reasonable message size after splitting IngestRecords to individual relays!")
                  throw new Exception("Failed relay of excessively large event(s)!")
                }

                logger.debug("Breaking %d ingest records into %d messages for relay still too large, splitting.".format(data.length, messages.size))
                encodeIngestMessages(ev.flatMap(_.split))
              }
            }

            val ingestRecords = data map { IngestRecord(eventIdSeq.next(offset), _) }
            encodeIngestMessages(List(IngestMessage(apiKey, path, authorities, ingestRecords, jobId, timestamp, storeMode)))

          case Authorized(event: Ingest, _, None) =>
            // cannot relay event without a resolved owner account ID; fail loudly.
            // this will abort the future, ensuring that state doesn't get corrupted
            sys.error("Unable to establish owner account ID for ingest of event " + event)

          case Authorized(archive @ Archive(apiKey, path, jobId, timestamp), offset, _) =>
            List(centralCodec.toMessage(ArchiveMessage(apiKey, path, jobId, eventIdSeq.next(offset), timestamp)))

          case Authorized(StoreFile(apiKey, path, _, jobId, content, timestamp, stream), offset, Some(authorities)) =>
            List(centralCodec.toMessage(StoreFileMessage(apiKey, path, authorities, Some(jobId), eventIdSeq.next(offset), content, timestamp, stream)))

          case Authorized(s: StoreFile, _, None) =>
            sys.error("Unable to establish owner account ID for storage of file " + s)
        }

        producer.send {
          new ProducerData[String, Message](centralTopic, identified)
        }
      } onFailure {
        case ex => logger.error("An error occurred forwarding messages from the local queue to central.", ex)
      } onSuccess {
        case _ => if (messages.nonEmpty) eventIdSeq.saveState(messages.last.offset)
      }
    } valueOr { error =>
      Promise successful {
        logger.error("Deserialization errors occurred reading events from Kafka: " + error.message)
      }
    }
  }

  private def deriveAuthority(event: Event): Future[Option[Authorities]] = event match {
    case Ingest(apiKey, path, writeAs, _, _, timestamp, _) =>
      if (writeAs.isDefined) Promise.successful(writeAs)
      else permissionsFinder.inferWriteAuthorities(apiKey, path, Some(timestamp))

    case StoreFile(apiKey, path, writeAs, _, _, timestamp, _) =>
      if (writeAs.isDefined) Promise successful writeAs
      else permissionsFinder.inferWriteAuthorities(apiKey, path, Some(timestamp))

    case _ => Promise.successful(None)
  }
}
