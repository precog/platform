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
    
    val run = for (shard <- yggShard) yield {
      Await.result(shard.start, 300 seconds)

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() { 
          Await.result(shard.stop, 300 seconds) 
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

  lazy val consumer = new KafkaConsumer(kafkaIngestConfig, routingActor)

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
