package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.kafka._
import com.precog.util._

import akka.util.Timeout
import akka.dispatch.{Future, Promise}
import akka.dispatch.ExecutionContext

import blueeyes.bkka._

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import _root_.kafka.message._
import _root_.kafka.producer._

import org.streum.configrity.{Configuration, JProperties}
import com.weiglewilczek.slf4s._ 

import scalaz._

class LocalKafkaEventStore(producer: Producer[String, Message], topic: String)(implicit executor: ExecutionContext) extends EventStore[Future] {
  def save(event: Event, timeout: Timeout) = Future {
    producer send {
      new ProducerData[String, Message](topic, new Message(EventEncoding.toMessageBytes(event)))
    }

    PrecogUnit
  }
}

object LocalKafkaEventStore {
  def apply(config: Configuration)(implicit executor: ExecutionContext): Option[(EventStore[Future], Stoppable)] = {
    val localTopic = config[String]("topic")

    val localProperties = {
      val props = JProperties.configurationToProperties(config)
      val host = config[String]("broker.host")
      val port = config[Int]("broker.port")
      props.setProperty("broker.list", "0:%s:%d".format(host, port))
      props
    }

    val producer = new Producer[String, Message](new ProducerConfig(localProperties))
    val stoppable = Stoppable.fromFuture(Future { producer.close })

    Some(new LocalKafkaEventStore(producer, localTopic) -> stoppable)
  }
}
