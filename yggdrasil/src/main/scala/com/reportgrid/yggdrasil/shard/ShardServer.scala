package com.reportgrid.yggdrasil
package shard 

import com.reportgrid.analytics.Path
import com.reportgrid.common._
import com.reportgrid.util._
import com.reportgrid.util.Bijection._
import kafka._
import leveldb._

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.ActorRef

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

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
    val metadataActor: ActorRef =  sys.error("todo") //ShardMetadata.dummyShardMetadataActor  

    val system = ActorSystem("Shard Actor System")

    val router = system.actorOf(Props(new RoutingActor(baseDir, routingTable, metadataActor)))
    
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
