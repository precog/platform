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
  def kafkaConsumerConfig: Properties = JProperties.configurationToProperties(config.detach("precog.kafka.consumer"))
}

class KafkaIngest(config: KafkaIngestConfig, router: ActorRef) extends Runnable with Logging {
  private lazy val consumer = initConsumer

  def initConsumer = {
    val consumerConfig= new ConsumerConfig(config.kafkaConsumerConfig)
    val consumer = Consumer.create(consumerConfig)
    consumer
  }

  def run {
    val rawEventsTopic = config.kafkaEventTopic 

    val streams = consumer.createMessageStreams(Map(rawEventsTopic -> 1))

    for(rawStreams <- streams.get(rawEventsTopic); stream <- rawStreams; message <- stream) {
      val msg = IngestMessageSerialization.read(message.payload)
      router ! msg 
    }
  }

  def requestStop {
    consumer.shutdown
  }
}


// vim: set ts=4 sw=4 et:
