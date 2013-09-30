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
import com.precog.common.security._
import com.precog.common.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.serialization._
import com.precog.shard.yggdrasil._

import com.precog.mimir._
import com.precog.mimir.memoization._

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.dispatch.ExecutionContext
import akka.dispatch.Await
import akka.util.Timeout
import akka.util.Duration
import akka.util.duration._

import blueeyes.json._

import java.io.File

import org.streum.configrity.Configuration

import java.util.concurrent.atomic.AtomicInteger

import scalaz._
import Scalaz._

trait JDBMPerformanceSpec extends Specification with PerformanceSpec {
  sequential 

  val timeout = Duration(5000, "seconds")

  val benchParams = BenchmarkParameters(5, 500, Some(500), false)
  val singleParams = BenchmarkParameters(5, 500, Some(500), false)

  val config = Configuration(Map.empty[String, String]) 

  val tmpDir = newTempDir() 
  lazy val shard = new TestShard(config, tmpDir)
  lazy val executor = new TestQueryExecutor(config, shard)

  val perfUtil = PerformanceUtil.default

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
          val event = Event(path, "apiKey", jval, Map.empty)
          batch(i) = EventMessage(EventId(pid, id), event)
          i += 1
          id += 1
        }
        val result = shard.storeBatch(batch, timeout)
        Await.result(result, timeout)
        
        b += 1
      }
    }
    
    "load test sim" in {
      insert(shard, Path("/test/query_set"), 2, 10000, 1)
      val threadCount = 10 
     
      val queries = List(
"(load(//test/query_set))",
"""
tests := load(//test/query_set)
count(tests where tests.gender = "male")
""",
"""
tests := load(//test/query_set)
histogram('platform) :=
  { platform: 'platform, num: count(tests where tests.platform = 'platform) }
histogram
"""
)

      def test(i: Int) = {
        val threads = (0.until(threadCount)) map { i =>
          if(i == 0) {
            new Thread {
              override def run() {
                insert(shard, Path("/test/insert_set"), 2, 10000, 1)
              }
            } 
          } else {
            new Thread {
              val rand = new java.util.Random()
              override def run() {
                var cnt = 0
                while(cnt < i) {
                  val q = queries(rand.nextInt(queries.length))  
                  val result = executor.execute("apiKey", q,"") 
                  result match {
                    case Success(jval) => 
                    case Failure(e) => new RuntimeException("Query result failure")
                  }
                  cnt += 1
                }
              }
            } 
          }
        }
        
        threads.foreach{ _.start }
        threads.foreach{ _.join }
      } 
      
      println("load test sim")
      val result = Performance().benchmark(test(10), benchParams, benchParams)   
      perfUtil.uploadResults("load_test_sim", result)
      //val result = Performance().profile(test(10))   
      
      result.report("load test sym", System.out)
      true must_== true
    }

    "insert" in {
      val tests = 100000
      val batchSize = 1000
      val result = Performance().benchmark(insert(shard, Path("/test/insert/"), 0, batchSize, tests / batchSize), singleParams, singleParams)   
      perfUtil.uploadResults("insert_100k", result)
      //val result = Performance().profile(insert(shard, Path("/test/insert/"), 0, batchSize, tests / batchSize))   

      println("starting insert test")
      result.report("insert 100K", System.out)
      true must_== true
    }
   
    def testRead() = {
      executor.execute("apiKey", "count(load(//test/large))")
    }

    "read large" in {
      insert(shard, Path("/test/large"), 1, 100000, 1)
      println("read large test")

      val result = Performance().benchmark(testRead(), benchParams, benchParams)   
      perfUtil.uploadResults("read_100k", result)
      //val result = Performance().profile(testRead())   
      result.report("read 100K", System.out)
      true must_== true
    }
    
    "read small 10K x 10" in {
      insert(shard, Path("/test/small1"), 1, 10000, 1)

      def test(i: Int) = {
        var cnt = 0
        while(cnt < i) {
          val result = executor.execute("apiKey", "count(load(//test/small1))") 
          result match {
            case Success(jval) =>
            case Failure(e) => new RuntimeException("Query result failure")
          }
          cnt += 1
        }
      }
      
      val result = Performance().benchmark(test(10), benchParams, benchParams)   
      perfUtil.uploadResults("read_10k_10x", result)
      //val result = Performance().profile(test(100))   
      
      result.report("read 10K elements x 10 times", System.out)
      true must_== true
    }
    
    "multi-thread read" in {
      insert(shard, Path("/test/small2"), 2, 10000, 1)
      val threadCount = 10 
      
      def test(i: Int) = {
        val threads = (0.until(threadCount)) map { _ =>
          new Thread {
            override def run() {
              var cnt = 0
              while(cnt < i) {
                val result = executor.execute("apiKey", "(load(//test/small2))") 
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
      
      val result = Performance().benchmark(test(1), benchParams, benchParams)   
      perfUtil.uploadResults("read_10k_10thread", result)
      //val result = Performance().profile(test(10))   
      
      println("read small thread test")
      result.report("read 10K elements x 1 times with 10 threads", System.out)
      true must_== true
    }
    
    "hw2 test 100K x 1" in {
      insert(shard, Path("/test/small3"), 1, 100000, 1)

      val query =
"""
tests := load(//test/small3)
count(tests where tests.gender = "male")
"""

      def test(i: Int) = {
        var cnt = 0
        while(cnt < i) {
          val result = executor.execute("apiKey", query) 
          result match {
            case Success(jval) =>
            case Failure(e) => new RuntimeException("Query result failure")
          }
          cnt += 1
        }
      }
      
      val result = Performance().benchmark(test(1), benchParams, benchParams)   
      perfUtil.uploadResults("hw2_100k", result)
      //val result = Performance().profile(test(100))   
      
      result.report("hw2 test 100K * 1", System.out)
      true must_== true
    }
    
    "hw3 test" in {
      insert(shard, Path("/test/small4"), 1, 100000, 1)

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
          val result = executor.execute("apiKey", query) 
          result match {
            case Success(jval) =>
            case Failure(e) => new RuntimeException("Query result failure")
          }
          cnt += 1
        }
      }
      
      val result = Performance().benchmark(test(1), benchParams, benchParams)   
      perfUtil.uploadResults("hw3_100k", result)
      //val result = Performance().profile(test(100))   
      
      result.report("hw3 test 100K * 1", System.out)
      true must_== true
    }
    
    "handle null scenario" in {
      val nullReal = """
[{
 "event":"activated",
 "currency":"USD",
 "customer":{
   "country":"CA",
   "email":"john@fastspring.com",
   "firstName":"John",
   "lastName":"Smith",
   "organization":"",
   "zipcode":"11111"
 },
 "endDate":null,
 "product":{
   "name":"Subscription 1"
 },
 "quantity":1,
 "regularPriceUsd":10,
 "timestamp":{
   "date":7,
   "day":3,
   "hours":0,
   "minutes":0,
   "month":2,
   "seconds":0,
   "time":1331078400000,
   "timezoneOffset":0,
   "year":112
 }
},{
 "event":"deactivated",
 "currency":"USD",
 "customer":{
   "country":"US",
   "email":"ryan@fastspring.com",
   "firstName":"Ryan",
   "lastName":"Dewell",
   "organization":"",
   "zipcode":"93101"
 },
 "endDate":{
   "date":7,
   "day":3,
   "hours":0,
   "minutes":0,
   "month":2,
   "seconds":0,
   "time":1331078400000,
   "timezoneOffset":0,
   "year":112
 },
 "product":{
   "name":"ABC Subscription"
 },
 "quantity":1,
 "reason":"canceled",
 "regularPriceUsd":9,
 "timestamp":{
   "date":7,
   "day":3,
   "hours":0,
   "minutes":0,
   "month":2,
   "seconds":0,
   "time":1331078400000,
   "timezoneOffset":0,
   "year":112
 }
}]
      """
      val jvals = JParser.parse(nullReal)
      val msgs = jvals match {
        case JArray(jvals) =>
          jvals.zipWithIndex.map {
            case (jval, idx) =>
              val event = Event(Path("/test/null"), "apiKey", jval, Map.empty)
              EventMessage(EventId(1,idx), event)
          }
      }

      Await.result(shard.storeBatch(msgs, timeout), timeout)

      Thread.sleep(10000)

      val result = executor.execute("apiKey", "load(//test/null)")
      result must beLike {
        case Success(JArray(vals)) => vals.size must_== 2
      }
    }.pendingUntilFixed
    "handle mixed type scenario" in {
      val mixedReal = """
[{
 "event":"activated",
 "currency":"USD",
 "customer":{
   "country":"CA",
   "email":"john@fastspring.com",
   "firstName":"John",
   "lastName":"Smith",
   "organization":"",
   "zipcode":"11111"
 },
 "endDate":"null",
 "product":{
   "name":"Subscription 1"
 },
 "quantity":1,
 "regularPriceUsd":10,
 "timestamp":{
   "date":7,
   "day":3,
   "hours":0,
   "minutes":0,
   "month":2,
   "seconds":0,
   "time":1331078400000,
   "timezoneOffset":0,
   "year":112
 }
},{
 "event":"deactivated",
 "currency":"USD",
 "customer":{
   "country":"US",
   "email":"ryan@fastspring.com",
   "firstName":"Ryan",
   "lastName":"Dewell",
   "organization":"",
   "zipcode":"93101"
 },
 "endDate":{
   "date":7,
   "day":3,
   "hours":0,
   "minutes":0,
   "month":2,
   "seconds":0,
   "time":1331078400000,
   "timezoneOffset":0,
   "year":112
 },
 "product":{
   "name":"ABC Subscription"
 },
 "quantity":1,
 "reason":"canceled",
 "regularPriceUsd":9,
 "timestamp":{
   "date":7,
   "day":3,
   "hours":0,
   "minutes":0,
   "month":2,
   "seconds":0,
   "time":1331078400000,
   "timezoneOffset":0,
   "year":112
 }
}]
      """
      val jvalues = JsonParser.parse(mixedReal)
      val msgs = jvalues match {
        case JArray(jvals) =>
          jvals.zipWithIndex.map {
            case (jval, idx) =>
              val event = Event(Path("/test/mixed"), "apiKey", jval, Map.empty)
              EventMessage(EventId(2,idx), event)
          }
      }

      Await.result(shard.storeBatch(msgs, timeout), timeout)
      
      Thread.sleep(10000)

      val result = executor.execute("apiKey", "load(//test/mixed)")
      result must beLike {
        case Success(JArray(vals)) => vals.size must_== 2
      }
    }.pendingUntilFixed
    
  }

  step {
    Await.result(shard.actorsStop, timeout) 
    cleanupTempDir(tmpDir)
  }
}

class TestQueryExecutor(config: Configuration, testShard: TestShard) extends 
    JDBMQueryExecutor with
    IterableDatasetOpsComponent { 

  override type Dataset[A] = IterableDataset[A]

  lazy val actorSystem = ActorSystem("testQueryExecutor")
  implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
  lazy val yggConfig = new JDBMQueryExecutorConfig {
      val config = TestQueryExecutor.this.config
      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir
      
      val clock = blueeyes.util.Clock.System
      val idSource = new FreshAtomicIdSource

      object valueSerialization extends SortSerialization[SValue] with SValueRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object eventSerialization extends SortSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object groupSerialization extends SortSerialization[(SValue, Identities, SValue)] with GroupRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object memoSerialization extends IncrementalSerialization[(Identities, SValue)] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization

      override lazy val flatMapTimeout: Duration = 5000 seconds
      override lazy val projectionRetrievalTimeout: Timeout = Timeout(5000 seconds)
      override lazy val maxEvalDuration: Duration = 5000 seconds
    }  
  type Storage = TestShard

  object ops extends Ops
  object query extends QueryAPI

  val storage = testShard
}

class TestShard(config: Configuration, dataDir: File) extends ActorYggShard[IterableDataset] with StandaloneActorEcosystem {
  type YggConfig = ProductionActorConfig
  lazy val yggConfig = new ProductionActorConfig {
    lazy val config = TestShard.this.config
  }
  lazy val yggState: YggState = YggState.restore(dataDir).unsafePerformIO.toOption.get 
  lazy val accessControl: AccessControl = new UnlimitedAccessControl()(ExecutionContext.defaultExecutionContext(actorSystem))
  def waitForRoutingActorIdle() {
    val td = Duration(5000, "seconds")
    implicit val to = new Timeout(td)
    Await.result(routingActor ? ControlledStop, td)
    Await.result(routingActor ? Start, td)
  }
}
