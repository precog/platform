package com.precog.yggdrasil
package shard
import kafka._

import com.precog.common._

import akka.actor.ActorRef
import com.weiglewilczek.slf4s.Logging
import java.util.Properties
import _root_.kafka.consumer._

class KafkaConsumer(props: Properties, router: ActorRef) extends Runnable with Logging {
  private lazy val consumer = initConsumer

  def initConsumer = {
    //logger.debug("Initializing kafka consumer")
    val config = new ConsumerConfig(props)
    val consumer = Consumer.create(config)
    //logger.debug("Kafka consumer initialized")
    consumer
  }

  def run {
    val rawEventsTopic = props.getProperty("precog.kafka.topic.raw", "raw")

    //logger.debug("Starting consumption from kafka queue: " + rawEventsTopic)

    val streams = consumer.createMessageStreams(Map(rawEventsTopic -> 1))

    for(rawStreams <- streams.get(rawEventsTopic); stream <- rawStreams; message <- stream) {
      //logger.debug("Processing incoming kafka message")
      val msg = IngestMessageSerialization.read(message.payload)
      router ! msg 
      //logger.debug("Serialized kafka message and sent to router")
    }
  }

  def requestStop {
    consumer.shutdown
  }
}


// vim: set ts=4 sw=4 et:
