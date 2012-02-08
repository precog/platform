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

import org.streum.configrity.Configuration

object KafkaShardServer extends Logging { 

  trait KafkaShardConfig extends YggConfig with KafkaIngestConfig

  def main(args: Array[String]) {
    val config = IO { Configuration.load(args(0)) } map { cfg => new KafkaShardConfig {
        def config = cfg
      }
    }
    
    val yggShard = config flatMap { cfg => YggState.restore(cfg.dataDir) map { (cfg, _) } } map { 
      case (cfg, Success(state)) =>
        new RealYggShard with KafkaIngester {
          val yggState = state 
          val yggConfig = cfg 
          val kafkaIngestConfig = cfg
        }
      case (cfg, Failure(e)) => sys.error("Error loading shard state from: %s".format(cfg.dataDir))
    }
   
    val timeout = 300 seconds

    val run = for (shard <- yggShard) yield {
     
      val startFuture = shard.start flatMap { _ => shard.startKafka }

      Await.result(startFuture, timeout)

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() {
          val stopFuture = shard.stopKafka flatMap { _ => shard.stop }
          Await.result(stopFuture, timeout)
        }
      })
    }

    run.unsafePerformIO
  }

}

trait KafkaIngester extends Logging {
  def kafkaIngestConfig: KafkaIngestConfig
  def routingActor: ActorRef

  implicit def executionContext: akka.dispatch.ExecutionContext

  lazy val consumer = new KafkaIngest(kafkaIngestConfig, routingActor)

  def startKafka = Future {
      if(kafkaIngestConfig.kafkaEnabled) { 
        new Thread(consumer).start
      }
    }

  import logger._

  def stopKafka = Future { debug("[Kafka Ingester] Stopping kafka consumer") } map
    { _ => consumer.requestStop } recover { case e => error("Error stopping kafka consumer", e) }
}

// vim: set ts=4 sw=4 et:
