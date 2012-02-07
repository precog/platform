package com.precog.yggdrasil
package shard
import kafka._

import com.precog.common._

import akka.actor.ActorRef
import com.weiglewilczek.slf4s.Logging
import java.util.Properties
import _root_.kafka.consumer._

import org.streum.configrity.JProperties

trait KafkaIngestConfig extends Config {
  def kafkaEnabled = config("precog.kafka.enabled", false) 
  def kafkaEventTopic = config[String]("precog.kafka.topic.events")
  def kafkaConsumerConfig: Properties = JProperties.configurationToProperties(config.detach("precog.kafak.consumer"))
}

class KafkaConsumer(config: KafkaIngestConfig, router: ActorRef) extends Runnable with Logging {
  private lazy val consumer = initConsumer

  def initConsumer = {
    //logger.debug("Initializing kafka consumer")
    val consumerConfig= new ConsumerConfig(config.kafkaConsumerConfig)
    val consumer = Consumer.create(consumerConfig)
    //logger.debug("Kafka consumer initialized")
    consumer
  }

  def run {
    val rawEventsTopic = config.kafkaEventTopic 

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
