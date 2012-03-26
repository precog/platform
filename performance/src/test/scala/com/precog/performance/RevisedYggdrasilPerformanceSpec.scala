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

import com.precog.common._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.shard.yggdrasil._

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.dispatch.ExecutionContext
import akka.dispatch.Await
import akka.util.Timeout
import akka.util.Duration
import akka.util.duration._

import java.io.File

import org.streum.configrity.Configuration

import java.util.concurrent.atomic.AtomicInteger

import scalaz._
import Scalaz._

trait RevisedYggdrasilPerformanceSpec extends Specification with PerformanceSpec {
  sequential 

  val timeout = Duration(5000, "seconds")

  val benchParams = BenchmarkParameters(5, 500, Some(500), true)

  val config = Configuration(Map.empty[String, String]) 

  val tmpDir = newTempDir() 
  lazy val shard = new TestShard(config, tmpDir)
  lazy val executor = new TestQueryExecutor(config, shard)
  
  step {    
    Await.result(shard.actorsStart, timeout)
  }

  "yggdrasil" should {

    val seqId = new AtomicInteger(0)

    def insert(shard: TestShard, path: Path, pid: Int, batchSize: Int, batches: Int) {

      val batch = new Array[EventMessage](batchSize)

      var id = 0
      var b = 0
      while(b < batches) {
        var i = 0
        while(i < batchSize) {
          val jval = AdSamples.adCampaignSample.sample.get
          val event = Event(path, "token", jval, Map.empty)
          batch(i) = EventMessage(EventId(pid, id), event)
          i += 1
          id += 1
        }
        val result = shard.storeBatch(batch, timeout)
        Await.result(result, timeout)
        
        b += 1
      }
      
      shard.waitForRoutingActorIdle
    }

    "insert" in {
      val tests = 10000
      val batchSize = 1000
      val result = Performance().benchmark(insert(shard, Path("/test/large/"), 0, batchSize, tests / batchSize), benchParams, benchParams)   

      println("starting insert test")
      result.report("insert 10K", System.out)
      true must_== true
    }
    
    "read large" in {
      val result = Performance().benchmark(executor.execute("token", "count(load(//test/large))"), benchParams, benchParams)   
      println("read large test")
      result.report("read 10K", System.out)
      true must_== true
    }
    
    "read small" in {
      insert(shard, Path("/test/small1"), 1, 100, 1)

      def test(i: Int) = {
        var cnt = 0
        while(cnt < i) {
          val result = executor.execute("token", "count(load(//test/small1))") 
          result match {
            case Success(jval) =>
            case Failure(e) => new RuntimeException("Query result failure")
          }
          cnt += 1
        }
      }
      
      val result = Performance().benchmark(test(100), benchParams, benchParams)   
      
      result.report("read 100 elements x 100 times", System.out)
      true must_== true
    }
    
    "multi-thread read" in {
      insert(shard, Path("/test/small2"), 2, 100, 1)
      val threadCount = 10 
      
      def test(i: Int) = {
        val threads = (0.until(threadCount)) map { _ =>
          new Thread {
            override def run() {
              var cnt = 0
              while(cnt < i) {
                val result = executor.execute("token", "(load(//test/small2))") 
                result match {
                  case Success(jval) =>
                  case Failure(e) => new RuntimeException("Query result failure")
                }
                cnt += 1
              }
            }
          } 
        }
        
        threads.foreach{ _.start }
        threads.foreach{ _.join }
      } 
      
      val result = Performance().benchmark(test(10), benchParams, benchParams)   
      
      println("read small thread test")
      result.report("read 100 elements x 10 times with 10 threads", System.out)
      true must_== true
    }
    
    "hw2 test" in {
      insert(shard, Path("/test/small3"), 1, 100, 1)

      val query =
"""
tests := load(//test/small3)
count(tests where tests.gender = "male")
"""

      def test(i: Int) = {
        var cnt = 0
        while(cnt < i) {
          val result = executor.execute("token", query) 
          result match {
            case Success(jval) =>
            case Failure(e) => new RuntimeException("Query result failure")
          }
          cnt += 1
        }
      }
      
      val result = Performance().benchmark(test(100), benchParams, benchParams)   
      
      result.report("hw2 test", System.out)
      true must_== true
    }
    
    "hw3 test" in {
      insert(shard, Path("/test/small4"), 1, 100, 1)

      val query =
"""
tests := load(//test/small4)
histogram('platform) :=
  { platform: 'platform, num: count(tests where tests.platform = 'platform) }
histogram
"""

      def test(i: Int) = {
        var cnt = 0
        while(cnt < i) {
          val result = executor.execute("token", query) 
          result match {
            case Success(jval) =>
            case Failure(e) => new RuntimeException("Query result failure")
          }
          cnt += 1
        }
      }
      
      val result = Performance().benchmark(test(100), benchParams, benchParams)   
      
      result.report("hw3 test", System.out)
      true must_== true
    }
  }

  step {
    Await.result(shard.actorsStop, timeout) 
    cleanupTempDir(tmpDir)
  }
}

class TestQueryExecutor(config: Configuration, testShard: TestShard) extends YggdrasilQueryExecutor {

  lazy val actorSystem = ActorSystem("test_query_executor")
  implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  lazy val yggConfig = new YggdrasilQueryExecutorConfig {
      val config = TestQueryExecutor.this.config
      val sortWorkDir = scratchDir
      val chunkSerialization = BinaryProjectionSerialization
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir
      override lazy val flatMapTimeout: Duration = 5000 seconds
      override lazy val projectionRetrievalTimeout: Timeout = Timeout(5000 seconds)
      override lazy val maxEvalDuration: Duration = 5000 seconds
    }  
  type Storage = TestShard
  object ops extends Ops
  object query extends QueryAPI

  val storage = testShard
}

class TestShard(config: Configuration, dataDir: File) extends ActorYggShard with StandaloneActorEcosystem {
  type YggConfig = ProductionActorConfig
  lazy val yggConfig = new ProductionActorConfig {
    lazy val config = TestShard.this.config
  }
  lazy val yggState: YggState = YggState.restore(dataDir).unsafePerformIO.toOption.get 

  def waitForRoutingActorIdle() {
    val td = Duration(5000, "seconds")
    implicit val to = new Timeout(td)
    Await.result(routingActor ? ControlledStop, td)
    Await.result(routingActor ? Restart, td)
  }
}
