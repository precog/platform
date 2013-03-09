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
import _root_.kafka.consumer._
import _root_.kafka.producer._
import _root_.kafka.message._

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

    val centralProperties: java.util.Properties = {
      val props = JProperties.configurationToProperties(centralConfig)
      props.setProperty("serializer.class", "com.precog.common.kafka.KafkaEventMessageCodec")
      props
    }

    val producer = new Producer[String, EventMessage](new ProducerConfig(centralProperties))

    val consumerHost = localConfig[String]("broker.host", "localhost")
    val consumerPort = localConfig[String]("broker.port", "9082").toInt
    val consumer = new SimpleConsumer(consumerHost, consumerPort, 5000, 64 * 1024)

    val relayAgent = new KafkaRelayAgent(permissionsFinder, eventIdSeq, consumer, localTopic, producer, centralTopic)
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
    producer: Producer[String, EventMessage], centralTopic: String,
    bufferSize: Int = 1024 * 1024, retryDelay: Long = 5000L,
    maxDelay: Double = 100.0, waitCountFactor: Int = 25)(implicit executor: ExecutionContext) extends Runnable with Logging {

  logger.info("Allocating KafkaRelayAgent, hash = " + hashCode)

  @volatile private var runnable = true;
  private val stopPromise = Promise[PrecogUnit]()
  private implicit val M: Monad[Future] = new FutureMonad(executor)

  def stop: Future[PrecogUnit] = Future({ runnable = false }) flatMap { _ => stopPromise }

  override def run() {
    while (runnable) {
      val offset = eventIdSeq.getLastOffset()
      logger.debug("Kafka consumer starting from offset: " + offset)

      try {
        ingestBatch(offset, 0, 0, 0)
      } catch {
        case ex: Exception => logger.error("Error in kafka consumer.", ex)
      }

      Thread.sleep(retryDelay)
    }

    stopPromise.success(PrecogUnit)
  }

  @tailrec
  private def ingestBatch(offset: Long, batch: Long, delay: Long, waitCount: Long) {
    if(batch % 100 == 0) logger.debug("Processing kafka consumer batch %d [%s]".format(batch, if(waitCount > 0) "IDLE" else "ACTIVE"))
    val fetchRequest = new FetchRequest(localTopic, 0, offset, bufferSize)

    val messages = consumer.fetch(fetchRequest)
    forwardAll(messages.toList)

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
        val identified: List[EventMessage] = messages.map {
          case Authorized(Ingest(apiKey, path, _, data, jobId, timestamp), offset, Some(authorities)) =>
            val ingestRecords = data map { IngestRecord(eventIdSeq.next(offset), _) }
            IngestMessage(apiKey, path, authorities, ingestRecords, jobId, timestamp)

          case Authorized(event: Ingest, _, None) =>
            // cannot relay event without a resolved owner account ID; fail loudly.
            // this will abort the future, ensuring that state doesn't get corrupted
            sys.error("Unable to establish owner account ID for ingest of event " + event)

          case Authorized(archive @ Archive(apiKey, path, jobId, timestamp), offset, _) =>
            ArchiveMessage(apiKey, path, jobId, eventIdSeq.next(offset), timestamp)
        }

        producer.send(new ProducerData[String, EventMessage](centralTopic, identified))
      } onFailure {
        case ex => logger.error("An error occurred forwarding messages from the local queue to central.", ex)
      } onSuccess {
        case _ => if (messages.nonEmpty) eventIdSeq.saveState(messages.last.offset)
      }
    } valueOr { error =>
      logger.error("Deserialization errors occurred reading events from Kafka: " + error.message)
    }
  }

  private def deriveAuthority(event: Event): Future[Option[Authorities]] = event match {
    case Ingest(apiKey, path, writeAs, _, _, timestamp) =>
      writeAs.map(a => Some(a).point[Future]).getOrElse(permissionsFinder.inferWriteAuthorities(apiKey, path, Some(timestamp)))

    case _ => Promise.successful(None)
  }

//  private def identify(event: Event, offset: Long, authorities: Option[Authorities]): Future[EventMessage] = {
//    event match {
//      case Ingest(apiKey, path, writeAs, data, jobId, timestamp) =>
//        writeAs.map(a => Some(a).point[Future]).getOrElse(permissionsFinder.inferWriteAuthorities(apiKey, path, Some(timestamp))) map {
//          case Some(authorities) =>
//            val ingestRecords = data map { IngestRecord(eventIdSeq.next(offset), _) }
//            IngestMessage(apiKey, path, authorities, ingestRecords, jobId, timestamp)
//
//          case None =>
//            // cannot relay event without a resolved owner account ID; fail loudly.
//            // this will abort the future, ensuring that state doesn't get corrupted
//            sys.error("Unable to establish owner account ID for ingest of event " + event)
//        }
//
//      case archive @ Archive(apiKey, path, jobId, timestamp) =>
//        Promise.successful(ArchiveMessage(apiKey, path, jobId, eventIdSeq.next(offset), timestamp))
//    }
//  }
}
