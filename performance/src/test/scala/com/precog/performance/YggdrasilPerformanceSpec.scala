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

import blueeyes.json.JsonParser
import blueeyes.json.JsonAST._

import java.io.File

import org.streum.configrity.Configuration

import scalaz._
import Scalaz._

trait YggdrasilPerformanceSpec extends Specification with PerformanceSpec {
  sequential 

  val timeout = Duration(5000, "seconds")

  val config = Configuration(Map.empty[String, String]) 

  val tmpDir = newTempDir() 
  lazy val shard = new TestShard(config, tmpDir)
  lazy val executor = new TestQueryExecutor(config, shard)
  
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

  step {    
    Await.result(shard.actorsStart, timeout)
  }

  "yggdrasil" should {
    "insert" in {
      performBatch(10000, 7000) { i =>
        val batchSize = 1000
        insert(shard, Path("/test/large/"), 0, batchSize, i / batchSize)   
      }
    }
    
    "read large" in {
      performBatch(10000, 4000) { i =>
        val result = executor.execute("token", "count(load(//test/large))") 
        result match {
          case Success(jval) => 
          case Failure(e) => new RuntimeException("Query result failure")
        }
      }
    }
    
    "read small" in {
      insert(shard, Path("/test/small1"), 1, 100, 1)
      
      performBatch(100, 5000) { i =>
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
    }
    
    "multi-thread read" in {
      insert(shard, Path("/test/small2"), 2, 100, 1)
      val threadCount = 10 

      performBatch(10, 2500) { i =>
        val threads = (0.until(threadCount)) map { _ =>
          new Thread {
            override def run() {
              var cnt = 0
              while(cnt < i) {
                val result = executor.execute("token", "count(load(//test/small2))") 
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
      val jvals = JsonParser.parse(nullReal)
      val msgs = jvals match {
        case JArray(jvals) =>
          jvals.zipWithIndex.map {
            case (jval, idx) =>
              val event = Event(Path("/test/null"), "token", jval, Map.empty)
              EventMessage(EventId(1,idx), event)
          }
        case _ => sys.error("Unexpected parse result")
      }
      Await.result(shard.storeBatch(msgs, timeout), timeout)

      val result = executor.execute("token", "load(//test/null)")
      result must beLike {
        case Success(JArray(vals)) => vals.size must_== 2
      }
    }

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
              val event = Event(Path("/test/mixed"), "token", jval, Map.empty)
              EventMessage(EventId(2,idx), event)
          }
        case _ => sys.error("Unexpected parse result")
      }
      Await.result(shard.storeBatch(msgs, timeout), timeout)
      
      val result = executor.execute("token", "load(//test/mixed)")
      result must beLike {
        case Success(JArray(vals)) => vals.size must_== 2
      }
    }
  }

  step {
    Await.result(shard.actorsStop, timeout) 
    cleanupTempDir(tmpDir)
  }
}
