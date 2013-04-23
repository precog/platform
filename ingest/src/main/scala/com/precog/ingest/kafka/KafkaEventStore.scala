package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.common.ingest._
import com.precog.common.kafka._
import com.precog.util._

import akka.util.Timeout
import akka.dispatch.{Future, Promise}
import akka.dispatch.ExecutionContext

import blueeyes.bkka._

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import _root_.kafka.common._
import _root_.kafka.message._
import _root_.kafka.producer._

import org.streum.configrity.{Configuration, JProperties}
import com.weiglewilczek.slf4s._

import scala.annotation.tailrec

import scalaz._
import scalaz.{ NonEmptyList => NEL }
import scalaz.syntax.std.option._

object KafkaEventStore {
  def apply(config: Configuration, permissionsFinder: PermissionsFinder[Future])(implicit executor: ExecutionContext): Validation[NEL[String], (EventStore[Future], Stoppable)] = {
    val localConfig = config.detach("local")
    val centralConfig = config.detach("central")
    println("Central config %s".format(centralConfig.toString()))
    println("centralConfig.get[String](\"zk.connect\")=%s".format(centralConfig.get[String]("zk.connect")))
    centralConfig.get[String]("zk.connect").toSuccess(NEL("central.zk.connect configuration parameter is required")) map { centralZookeeperHosts =>
      val serviceUID = ZookeeperSystemCoordination.extractServiceUID(config)
      val coordination = ZookeeperSystemCoordination(centralZookeeperHosts, serviceUID, yggCheckpointsEnabled = true)
      val agent = serviceUID.hostId + serviceUID.serviceId

      val eventIdSeq = SystemEventIdSequence(agent, coordination)
      val Some((eventStore, esStop)) = LocalKafkaEventStore(localConfig)

      val stoppables = if (config[Boolean]("relay_data", true)) {
        val (_, raStop) = KafkaRelayAgent(permissionsFinder, eventIdSeq, localConfig, centralConfig)
        esStop.parent(raStop)
      } else esStop

      (eventStore, stoppables)
    }
  }
}

class LocalKafkaEventStore(producer: Producer[String, Message], topic: String, maxMessageSize: Int, messagePadding: Int)(implicit executor: ExecutionContext) extends EventStore[Future] with Logging {
  logger.info("Creating LocalKafkaEventStore for %s with max message size = %d".format(topic, maxMessageSize))
  private[this] val codec = new KafkaEventCodec

  def save(event: Event, timeout: Timeout) = Future {
    def genEvents(ev: List[Event], ingest: Ingest): List[Message] = {
      @tailrec def rec(ev: List[Event]): List[Message] = {
        val messages = ev.map(codec.toMessage)

        if (messages.forall(_.size + messagePadding <= maxMessageSize)) {
          messages
        } else {
          if (ev.size == event.length) {
            logger.error("Failed to reach reasonable message size after splitting JValues to individual inserts!")
            throw new Exception("Failed insertion of excessively large event(s)!")
          }

          logger.debug("Breaking %d events into %d messages still too large, splitting.".format(ingest.length, messages.size))
          rec(ev.flatMap(_.split(2, java.util.UUID.randomUUID)))
        }
      }

      rec(ev)
    }

    val toSend: List[Message] = event.fold(
      ingest => genEvents(List(event), ingest),
      archive => List(codec.toMessage(archive)),
      storeFile => sys.error("todo")
    )

    producer send {
      new ProducerData[String, Message](topic, toSend)
    }
    PrecogUnit
  }
}

object LocalKafkaEventStore {
  def apply(config: Configuration)(implicit executor: ExecutionContext): Option[(EventStore[Future], Stoppable)] = {
    val localTopic = config[String]("topic")
    val maxMessageSize = config[Int]("broker.max_message_size", 1000000)
    val messagePadding = config[Int]("message_padding", 100)

    val localProperties: java.util.Properties = {
      val props = JProperties.configurationToProperties(config)
      val host = config[String]("broker.host")
      val port = config[Int]("broker.port")
      props.setProperty("broker.list", "0:%s:%d".format(host, port))
      //props.setProperty("serializer.class", "com.precog.common.kafka.KafkaEventCodec")
      props.setProperty("max.message.size", maxMessageSize.toString)
      props
    }

    val producer = new Producer[String, Message](new ProducerConfig(localProperties))
    val stoppable = Stoppable.fromFuture(Future { producer.close })

    Some(new LocalKafkaEventStore(producer, localTopic, maxMessageSize, messagePadding) -> stoppable)
  }
}
