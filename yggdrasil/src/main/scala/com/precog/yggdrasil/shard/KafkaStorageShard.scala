package com.precog.yggdrasil
package shard
import kafka._

import akka.actor.ActorRef
import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.duration._

import java.io._
import java.util.Properties

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.effect._

object KafkaShardServer extends Logging { 
  def main(args: Array[String]) {
    val storageShardConfig = readProperties(args(0))
    
    val storageShard: IO[StorageShard] = {
      ShardConfig.fromProperties(storageShardConfig) map {
        case Success(config) => new FilesystemBootstrapStorageShard with KafkaStorageShard {
          val shardConfig = config
        }

        case Failure(e) => sys.error("Error loading shard config: " + e)
      }
    }

    val run = for (shard <- storageShard) yield {
      Await.result(shard.start, 300 seconds)

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() { 
          Await.result(shard.stop, 300 seconds) 
        }
      })
    }

    run.unsafePerformIO
  }

  def readProperties(filename: String) = {
    val props = new Properties
    props.load(new FileReader(filename))
    props
  }

  def defaultProperties = {
    val config = new Properties() 
    
    // local storage root dir required for metadata and leveldb data 
    config.setProperty("precog.storage.root", "/tmp/repl_test_storage") 
    
    // Insert a random selection of events (events per class, number of classes) 
    //config.setProperty("precog.test.load.dummy", "1000,10") 
     
    // kafka ingest consumer configuration 
    config.setProperty("precog.kafka.enable", "true") 
    config.setProperty("precog.kafka.topic.raw", "test_topic_1") 
    config.setProperty("groupid","test_group_1") 
     
    config.setProperty("zk.connect","127.0.0.1:2181") 
    config.setProperty("zk.connectiontimeout.ms","1000000") 
    
    config 
  }
}

trait KafkaStorageShard extends StorageShard with Logging with ShardLogging {
  def shardConfig: ShardConfig
  def routingActor: ActorRef
  implicit def executionContext: akka.dispatch.ExecutionContext

  lazy val consumer = new KafkaConsumer(shardConfig.properties, routingActor)

  abstract override def start = super.start flatMap { _ =>
    Future {
      if(shardConfig.properties.getProperty("precog.kafka.enable", "false").trim.toLowerCase == "true") {
        new Thread(consumer).start
      }
    }
  }

  abstract override def stop = {
    Future { ld("Stopping kafka consumer") } map
    { _ => consumer.requestStop } recover rle("Error stopping kafka consumer") flatMap
    { _ => super.stop }
  }
}

// vim: set ts=4 sw=4 et:
