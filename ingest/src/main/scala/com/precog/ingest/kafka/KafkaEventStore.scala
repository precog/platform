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
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import _root_.kafka.common._
import _root_.kafka.message._
import _root_.kafka.producer._

import org.streum.configrity.{Configuration, JProperties}
import com.weiglewilczek.slf4s._

import scala.annotation.tailrec

import scalaz._
import scalaz.{ NonEmptyList => NEL }
import scalaz.\/._
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
  private implicit val M = new blueeyes.bkka.FutureMonad(executor)

  def save(event: Event, timeout: Timeout) = {
    @tailrec def encodeAll(toEncode: List[Event], messages: Vector[Message]): StoreFailure \/ Vector[Message] = {
      toEncode match {
        case x :: xs =>
          val message = codec.toMessage(x)
          if (message.size + messagePadding <= maxMessageSize) {
            encodeAll(xs, messages :+ message)
          } else {
            val postSplit = x.split(2) 
            if (postSplit.length == 1) {
              logger.error("Failed to reach reasonable message size for event: %s".format(event))
              left(StoreFailure("Failed insertion due to excessively large event(s)!"))
            } else {
              encodeAll(postSplit ::: xs, messages)
            }
          }

        case Nil => right(messages)
      }
    }

    val toSend = event.fold(
      ingest => encodeAll(List(event), Vector.empty),
      archive => right(List(codec.toMessage(archive))),
      storeFile => encodeAll(List(event), Vector.empty)
    )

    toSend traverse { kafkaMessages =>
      Future {
        producer send { new ProducerData[String, Message](topic, kafkaMessages) }
        PrecogUnit
      }
    }
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
