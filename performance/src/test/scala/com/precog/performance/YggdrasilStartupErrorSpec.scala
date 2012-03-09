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
package com.precog.performance

import org.specs2.mutable.Specification

import com.precog.analytics.Path
import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.shard.yggdrasil._

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import org.streum.configrity.Configuration

import scalaz._
import Scalaz._

trait YggdrasilStartupErrorSpec extends Specification with PerformanceSpec {
  sequential 

  val timeout = Duration(30, "seconds")
  val tmpFile = File.createTempFile("insert_test", "_db")
 
  val config = Configuration.parse("""
        precog {
          kafka {
            enabled = true 
            topic {
              events = central_event_store
            }
            consumer {
              zk {
                connect = devqclus03.reportgrid.com:2181 
                connectiontimeout {
                  ms = 1000000
                }
              }
              groupid = shard_consumer
            }
          }
        }
        kafka {
          batch {
            host = devqclus03.reportgrid.com 
            port = 9092
            topic = central_event_store          }
        }
        zookeeper {
          hosts = devqclus03.reportgrid.com:2181
          basepath = [ "com", "precog", "ingest", "v1" ]          prefix = test
        } 
      """)  

  step {    
    tmpFile.delete
    tmpFile.mkdirs
  }

  "yggdrasil" should {
    def insert(shard: TestShard, path: Path, batchSize: Int, batches: Int) {

      val batch = new Array[EventMessage](batchSize)

      var id = 0
      var b = 0
      while(b < batches) {
        var i = 0
        while(i < batchSize) {
          val jval = AdSamples.adCampaignSample.sample.get
          val event = Event(path, "token", jval, Map.empty)
          batch(i) = EventMessage(EventId(0, id), event)
          i += 1
          id += 1
        }
        val result = shard.storeBatch(batch, timeout)
        Await.result(result, timeout)
        b += 1
      }
    }

    "insert 10K elements in 7s".performBatch(1000, 1000) { i =>
      val batchSize = 1000
 
      val shard = new TestShard(config, tmpFile)
      Await.result(shard.actorsStart, timeout)
      insert(shard, Path("/test/large/"), batchSize, i / batchSize)   
      Await.result(shard.actorsStop, timeout)
    }

    "not fail during startup" in {
      val shard = new TestShard(config, tmpFile)
      val executor = new TestQueryExecutor(config, shard)
      val t = new Thread() {
        override def run() {
          val limit = 100
          var cnt = 0 
          while(cnt < limit) {
            val result = executor.execute("token", "count(load(//test/small1))")
            cnt += 1
          }
        }
      }
      t.start()
      Await.result(shard.actorsStart, timeout)
      t.join
      Await.result(shard.actorsStop, timeout)
      success
    }

  }

  step {
    def delDir(dir : File) {
      dir.listFiles.foreach {
        case d if d.isDirectory => delDir(d)
        case f => f.delete()
      }
      dir.delete()
    }
    delDir(tmpFile)
  }
}
