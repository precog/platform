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
