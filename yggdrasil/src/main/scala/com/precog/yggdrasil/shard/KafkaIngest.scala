package com.precog.yggdrasil
package shard

import kafka._

import com.precog.common._
import com.precog.common.kafka._
import com.precog.common.util._

import akka.actor.ActorRef
import akka.dispatch.MessageDispatcher

import com.weiglewilczek.slf4s.Logging

import java.util.Properties

import _root_.kafka.consumer._
import _root_.kafka.message._

import org.streum.configrity.JProperties

import scalaz._
import Scalaz._

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

class NewKafkaIngest(checkpoints: YggCheckpoints, config: KafkaIngestConfig, router: ActorRef)(implicit dispatcher: MessageDispatcher) extends Logging {

  private lazy val ingester = {
    val batchConsumer = new KafkaBatchConsumer("devqclus03.reportgrid.com", 9092, config.kafkaEventTopic)
    new KafkaBatchIngester(batchConsumer)(ingestMessages _)
  }

  def start() = ingester.start(checkpoints.latestCheckpoint.offset)

  def stop() = ingester.stop()

  def ingestMessages(messages: List[MessageAndOffset]) {
    if(!messages.isEmpty) {
      messages.foreach { msg =>
        router ! IngestMessageSerialization.read(msg.message.payload)
      }
      val newOffset = messages.last.offset
      checkpoints.messagesConsumed(YggCheckpoint(newOffset, VectorClock.empty)) 
    }
  }
}

// shard ingest flow
// - consume from kafka
// - pass to router 
// -- split to metadata and leveldb
// -- when in leveldb notify metadata
// 
// other issues
// - when is data 'in leveldb'
// - what is the last metadata safe point

trait YggCheckpoints {

  protected var lastCheckpoint = YggCheckpoint(0L, VectorClock.empty)
  private var pendingCheckpoints = Vector[YggCheckpoint]()

  def messagesConsumed(checkpoint: YggCheckpoint) {
    pendingCheckpoints = pendingCheckpoints :+ checkpoint 
  }

  def metadataPersisted(messageClock: VectorClock) {
    val (before, after) = pendingCheckpoints.span { 
      _.messageClock.isLowerBoundOf(messageClock) 
    }

    if(before.size > 0) {
      val nextCheckpoint = before.last
      
      saveRecoveryPoint(nextCheckpoint)
      
      lastCheckpoint = nextCheckpoint
      pendingCheckpoints = after
    }
  }
  
  def latestCheckpoint() = lastCheckpoint

  def pendingCheckpointCount() = pendingCheckpoints.size

  def pendingCheckpointValue() = pendingCheckpoints

  protected def saveRecoveryPoint(checkpoint: YggCheckpoint): Unit
}

class TestYggCheckpoints extends YggCheckpoints with Logging {
  protected def saveRecoveryPoint(checkpoint: YggCheckpoint) {
    logger.info("[PLACEHOLDER - TODO] saving shard recovery point to zookeeper. " + checkpoint)
  } 
}

class SystemCoordinationYggCheckpoints(shard: String, coordination: SystemCoordination) extends YggCheckpoints with Logging {
  
  lastCheckpoint = coordination.loadYggCheckpoint(shard) match {
    case Success(checkpoint) => checkpoint
    case Failure(e)          => sys.error("Error loading shard checkpoint from zookeeper")
  }
  
  override def messagesConsumed(checkpoint: YggCheckpoint) {
    logger.debug("Recording new consumption checkpoint: " + checkpoint)
    super.messagesConsumed(checkpoint)
  }

  override def metadataPersisted(messageClock: VectorClock) {
    logger.debug("Recording new metadata checkpoint: " + messageClock)
    super.metadataPersisted(messageClock)
  }

  protected def saveRecoveryPoint(checkpoint: YggCheckpoint) {
    coordination.saveYggCheckpoint(shard, checkpoint)    
  } 
}

// vim: set ts=4 sw=4 et:
