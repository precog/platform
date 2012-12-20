package com.precog.ingest
package kafka

import com.precog.common._
import com.precog.common.ingest._
import com.precog.util._

import akka.util.Timeout
import akka.dispatch.{Future, Promise}
import akka.dispatch.MessageDispatcher

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import _root_.kafka.producer._

import org.streum.configrity.{Configuration, JProperties}
import com.weiglewilczek.slf4s._ 

import scalaz._

class KafkaEventStore(router: EventRouter, producerId: Int, firstEventId: Int = 0)(implicit dispatcher: MessageDispatcher) extends EventStore {
  private val nextEventId = new AtomicInteger(firstEventId)
  
  def save(action: Action, timeout: Timeout) = {
    val actionId = nextEventId.incrementAndGet
    action match {
      case event : Event => router.route(EventMessage(producerId, actionId, event)) map { _ => PrecogUnit }
      case archive : Archive => router.route(ArchiveMessage(producerId, actionId, archive)) map { _ => PrecogUnit }
    }
  }

  def start(): Future[PrecogUnit] = Promise.successful(PrecogUnit)

  def stop(): Future[PrecogUnit] = router.close.map(_ => PrecogUnit)
}

class LocalKafkaEventStore(config: Configuration)(implicit dispatcher: MessageDispatcher) extends EventStore with Logging {
  private val localTopic = config[String]("topic")
  private val localProperties = {
    val props = JProperties.configurationToProperties(config)
    val host = config[String]("broker.host")
    val port = config[Int]("broker.port")
    props.setProperty("broker.list", "0:%s:%d".format(host, port))
    props
  }

  private val producer = new Producer[String, IngestMessage](new ProducerConfig(localProperties))

  def start(): Future[PrecogUnit] = Promise.successful(PrecogUnit)

  def save(action: Action, timeout: Timeout) = Future {
    val msg = action match {
      case event : Ingest => EventMessage(-1, -1, event)
      case archive : Archive => ArchiveMessage(-1, -1, archive)
    }
    val data = new ProducerData[String, IngestMessage](localTopic, msg)
    producer.send(data)
    PrecogUnit
  }

  def stop(): Future[PrecogUnit] = Future { producer.close; PrecogUnit } 
}
