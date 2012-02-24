/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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

  private lazy val consumer = {
    new KafkaConsumer("devqclus03.reportgrid.com", 9092, config.kafkaEventTopic)(ingestMessages _)
  }

  def start() = consumer.start(checkpoints.latestCheckpoint.offset)

  def stop() = consumer.stop()

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
  def messagesConsumed(checkpoint: YggCheckpoint): Unit
  def metadataPersisted(messageClock: VectorClock): Unit
  def latestCheckpoint(): YggCheckpoint
}

class TestYggCheckpoints extends YggCheckpoints with Logging {

  // invariants
  // - metadata must NEVER be ahead of leveldb state
  // - zookeeper state must NEVER be ahead of metadata

  private var lastCheckpoint = YggCheckpoint(0L, VectorClock.empty)
  private var pendingCheckpoints = Vector[YggCheckpoint]()

  def messagesConsumed(checkpoint: YggCheckpoint) {
    pendingCheckpoints = pendingCheckpoints :+ checkpoint 
  }

  def metadataPersisted(messageClock: VectorClock) {
    val (before, after) = pendingCheckpoints.span { 
      _.messageClock.lessThanOrEqual(messageClock) 
    }

    if(before.size > 0) {
      val nextCheckpoint = before.last
      saveRecoveryPoint(nextCheckpoint)
      pendingCheckpoints = after
    }
  }

  def latestCheckpoint() = lastCheckpoint

  private def saveRecoveryPoint(checkpoint: YggCheckpoint) {
    logger.info("[PLACEHOLDER - TODO] saving shard recovery point to zookeeper. " + checkpoint)
    lastCheckpoint = checkpoint
  } 
}

class SystemCoordinationYggCheckpoints(shard: String, coordination: SystemCoordination) extends YggCheckpoints {
  
  private var lastCheckpoint = coordination.loadYggCheckpoint(shard) match {
    case Success(checkpoint) => checkpoint
    case Failure(e)          => sys.error("Error loading shard checkpoint from zookeeper")
  }
  
  private var pendingCheckpoints = Vector[YggCheckpoint]()

  def messagesConsumed(checkpoint: YggCheckpoint) {
    pendingCheckpoints = pendingCheckpoints :+ checkpoint 
    // this is just a hack until I finish the true sync issues
    // the intention here is prevent the load test env from
    // not starting at a 0 offset every time it is clear that
    // this may result in data loss
    saveRecoveryPoint(checkpoint)
  }

  def metadataPersisted(messageClock: VectorClock) {
    val (before, after) = pendingCheckpoints.span { 
      _.messageClock.lessThanOrEqual(messageClock) 
    }

    if(before.size > 0) {
      val nextCheckpoint = before.last
      saveRecoveryPoint(nextCheckpoint)
      pendingCheckpoints = after
    }
  }

  def latestCheckpoint() = lastCheckpoint

  private def saveRecoveryPoint(checkpoint: YggCheckpoint) {
    coordination.saveYggCheckpoint(shard, checkpoint)    
    lastCheckpoint = checkpoint
  } 
}

// vim: set ts=4 sw=4 et:
