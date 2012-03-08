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
