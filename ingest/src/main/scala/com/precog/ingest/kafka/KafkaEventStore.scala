package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.kafka._
import com.precog.util._

import akka.util.Timeout
import akka.dispatch.{Future, Promise}
import akka.dispatch.MessageDispatcher

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import _root_.kafka.message._
import _root_.kafka.producer._

import org.streum.configrity.{Configuration, JProperties}
import com.weiglewilczek.slf4s._ 

import scalaz._

class LocalKafkaEventStore(config: Configuration)(implicit dispatcher: MessageDispatcher) extends EventStore with Logging {
  private val localTopic = config[String]("topic")
  private val localProperties = {
    val props = JProperties.configurationToProperties(config)
    val host = config[String]("broker.host")
    val port = config[Int]("broker.port")
    props.setProperty("broker.list", "0:%s:%d".format(host, port))
    props
  }

  private val producer = new Producer[String, Message](new ProducerConfig(localProperties))

  def start(): Future[PrecogUnit] = Promise.successful(PrecogUnit)

  def save(event: Event, timeout: Timeout) = Future {
    producer send {
      new ProducerData[String, Message](localTopic, new Message(EventEncoding.toMessageBytes(event)))
    }

    PrecogUnit
  }

  def stop(): Future[PrecogUnit] = Future { producer.close; PrecogUnit } 
}
