package com.precog
package ingest
package kafka

import common._

import java.util.Properties

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import _root_.kafka.producer._

class KafkaMessaging(topic: String, config: Properties)(implicit dispatcher: MessageDispatcher) extends Messaging {
 
  val producer = new Producer[String, IngestMessage](new ProducerConfig(config))

  def send(address: MailboxAddress, msg: EventMessage) = {
    Future {
      val data = new ProducerData[String, IngestMessage](topic, msg)
      try {
        producer.send(data)
      } catch {
        case x => x.printStackTrace(System.out); throw x
      }
    }
  }

  def close() = {
    Future {
      producer.close()
    }
  }

}

