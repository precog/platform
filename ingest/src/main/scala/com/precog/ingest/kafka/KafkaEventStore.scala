package com.precog
package ingest
package kafka

import akka.util.Timeout

import common._
import util._
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
  
  def save(action: Action, timeout: Timeout) = {
    val actionId = nextEventId.incrementAndGet
    action match {
      case event : Event => router.route(EventMessage(producerId, actionId, event)) map { _ => () }
      case archive : Archive => router.route(ArchiveMessage(producerId, actionId, archive)) map { _ => () }
    }
  }

  def start(): Future[Unit] = Future { () }

  def stop(): Future[Unit] = router.close.map(_ => ())
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

  def start(): Future[Unit] = Future { () } 

  def save(action: Action, timeout: Timeout) = Future {
    val msg = action match {
      case event : Event => EventMessage(-1, -1, event)
      case archive : Archive => ArchiveMessage(-1, -1, archive)
    }
    val data = new ProducerData[String, IngestMessage](localTopic, msg)
    producer.send(data)
  }

  def stop(): Future[Unit] = Future { producer.close } 
}
