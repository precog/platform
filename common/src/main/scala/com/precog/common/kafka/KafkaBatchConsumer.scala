package com.precog
package common 
package kafka

import scala.annotation.tailrec

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s._ 

import _root_.kafka.api._
import _root_.kafka.consumer._
import _root_.kafka.message._

trait BatchConsumer {
  def ingestBatch(offset: Long, bufferSize: Int): Iterable[MessageAndOffset]
  def close(): Unit
}

object BatchConsumer {
  val NullBatchConsumer = new BatchConsumer {
    def ingestBatch(offset: Long, bufferSize: Int) = List()
    def close() = ()
  }
}

class KafkaBatchConsumer(host: String, port: Int, topic: String) extends BatchConsumer {
 
  private val timeout = 5000
  private val buffer = 64 * 1024

  private lazy val consumer = new SimpleConsumer(host, port, timeout, buffer) 

  def ingestBatch(offset: Long, bufferSize: Int): MessageSet = {
    val fetchRequest = new FetchRequest(topic, 0, offset, bufferSize)

    consumer.fetch(fetchRequest)
  }

  def close() {
    consumer.close
  }
}
