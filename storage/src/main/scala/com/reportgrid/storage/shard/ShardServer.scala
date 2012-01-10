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
package com.reportgrid.storage
package shard 

import kafka._
import leveldb._
import Bijection._

import akka.actor.Actor
import akka.actor.ActorRef

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.reportgrid.common._
import com.reportgrid.analytics.Path
import com.weiglewilczek.slf4s._

import _root_.kafka.consumer._

import java.io.File
import java.io.FileInputStream
import java.io.BufferedReader
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec

import scalaz._
import scalaz.syntax.std.optionV._
import scalaz.syntax.validation._
import scalaz.effect._
import scalaz.iteratee.EnumeratorT

// On startup
// - walk directory structure loading metadata
// - create top level metadata (structural questions etc)
// -- specific structural questions
// --- vfs path explore (vfs paths how about selectors?)
// --- if selectors aren't included above then a separate api is (selector children)

class ShardServer {
  def run(props: Properties): Unit = {
   
    val baseDir = new File(props.getProperty("querio.storage.root", "."))
    val routingTable = new SingleColumnProjectionRoutingTable
    val metadataActor = ShardMetadata.dummyShardMetadataActor  

    val router = Actor.actorOf(new RoutingActor(baseDir, routingTable, metadataActor))
    
    val consumer = new KafkaConsumer(props, router)

    val consumerThread = new Thread(consumer)
    consumerThread.start

    println("Shard Server started...")
   
  }
}

class KafkaConsumer(props: Properties, router: ActorRef) extends Runnable {
  private lazy val consumer = initConsumer

  def initConsumer = {
    val config = new ConsumerConfig(props)
    Consumer.create(config)
  }

  def run {
    val rawEventsTopic = props.getProperty("querio.storage.topic.raw", "raw")

    val streams = consumer.createMessageStreams(Map(rawEventsTopic -> 1))

    for(rawStreams <- streams.get(rawEventsTopic); stream <- rawStreams; message <- stream) {
      router ! IngestMessageSerialization.readMessage(message.buffer) 
    }
  }

  def requestStop {
    consumer.shutdown
  }
}

object ShardServer {
  def main(args: Array[String]) = {

    val server = new ShardServer

    server.run(loadProperties(args))
  }

  def loadProperties(args: Array[String]): Properties = {
    val propFile = new File(args(0))
    if (!propFile.exists) {
      println("Kafka properties file not found: " + propFile)
      sys.exit(1)
    }

    val props = new Properties()
    val propStream = new FileInputStream(propFile)
    try {
      props.load(propStream) 
    } finally {
      propStream.close()
    }
    props
  }
}

// vim: set ts=4 sw=4 et:
