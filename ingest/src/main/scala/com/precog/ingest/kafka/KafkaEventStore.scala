package com.precog
package ingest
package kafka

import akka.util.Timeout

import common._
import common.util._
import ingest.util._

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s._ 

import scalaz._
import Scalaz._

import _root_.kafka.producer._

import org.streum.configrity.{Configuration, JProperties}

class KafkaEventStore(router: EventRouter, producerId: Int, firstEventId: Int = 0)(implicit dispatcher: MessageDispatcher) extends EventStore {
  private val nextEventId = new AtomicInteger(firstEventId)
  
  def save(event: Event, timeout: Timeout) = {
    val eventId = nextEventId.incrementAndGet
    router.route(EventMessage(producerId, eventId, event)) map { _ => () }
  }

  def start(): Future[Unit] = Future { () }

  def stop(): Future[Unit] = router.close
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

  private val producer = new Producer[String, Event](new ProducerConfig(localProperties))

  def start(): Future[Unit] = Future { () } 

  def save(event: Event, timeout: Timeout) = Future {
    val data = new ProducerData[String, Event](localTopic, event)
    producer.send(data)
  }

  def stop(): Future[Unit] = Future { producer.close } 

}

