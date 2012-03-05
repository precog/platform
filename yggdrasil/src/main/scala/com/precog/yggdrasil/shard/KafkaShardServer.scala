package com.precog.yggdrasil
package shard

import kafka._

import com.precog.yggdrasil.kafka._
import com.precog.common.kafka._

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
  
  trait KafkaShardServerConfig extends
     ProductionActorConfig with
     BaseConfig 

  def main(args: Array[String]) {
    val config = IO {  
      new KafkaShardServerConfig {
        val config = Configuration.load(args(0))
      }
    }
    
    val yggShard = for {
      cfg <- config
      restorationResult <- YggState.restore(cfg.dataDir)
    } yield {
      restorationResult match {
        case Success(state) =>
          new ActorYggShard with 
            ProductionActorEcosystem with
            YggConfigComponent {
            type YggConfig = KafkaShardServerConfig 
            val yggState = state 
            val yggConfig = cfg 
            val kafkaIngestConfig = cfg
            val batchConsumer = BatchConsumer.NullBatchConsumer 
            val yggCheckpoints = new TestYggCheckpoints
          }

        case Failure(e) => 
          sys.error("Error loading shard state from: %s".format(cfg.dataDir))
      }
    }
   
    val timeout = 300 seconds

    val run = for (shard <- yggShard) yield {
      
      Await.result(shard.start, timeout)

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() { Await.result(shard.stop, timeout) }
      })
    }

    run.unsafePerformIO
  }
}

// vim: set ts=4 sw=4 et:
