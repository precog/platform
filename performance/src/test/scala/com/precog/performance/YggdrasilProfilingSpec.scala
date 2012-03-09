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
import akka.util.Timeout
import akka.util.Duration
import akka.util.duration._

import java.io.File

import org.streum.configrity.Configuration

import scalaz._
import Scalaz._

trait YggdrasilProfilingSpec extends Specification with PerformanceSpec {

  "yggdrasil" should {
    sequential 

    val timeout = Duration(5000, "seconds")

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
              topic = central_event_store
            }
          }
          zookeeper {
            hosts = devqclus03.reportgrid.com:2181
            basepath = [ "com", "precog", "ingest", "v1" ]
            prefix = test
          } 
        """)  
    val tmpDir = newTempDir() 
    lazy val shard = new TestShard(config, tmpDir)
    lazy val executor = new TestQueryExecutor(config, shard)
    
    step {    
      Await.result(shard.actorsStart, timeout)
    }

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
    }

//    "insert" in {
//      performBatch(10000, 7000) { i =>
//        val batchSize = 1000
//        var cnt = 0
//        while(true) {
//          println("Test " + cnt)
//          insert(shard, Path("/test/large/"), cnt, batchSize, i / batchSize)   
//          cnt += 1
//        }
//      }
//    }
    
//    "read large" in {
//      performBatch(10000, 4000) { i =>
//        var cnt = 0 
//        while(true) {
//          println("Test " + cnt)
//          val result = executor.execute("token", "count(load(//test/large))") 
//          result match {
//            case Success(jval) => 
//            case Failure(e) => new RuntimeException("Query result failure")
//          }
//          cnt += 1
//        }
//      }
//    }
    
//    "read small" in {
//      insert(shard, Path("/test/small1"), 1, 100, 1)
//      
//      performBatch(100, 5000) { i =>
//      
//        var cnt = 0
//        while(true) {
//          if(cnt % 100 == 0) println("Test " + cnt)
//          val result = executor.execute("token", "count(load(//test/small1))") 
//          result match {
//            case Success(jval) =>
//            case Failure(e) => new RuntimeException("Query result failure")
//          }
//          cnt += 1
//        }
//      }
//    }
//    
//    "multi-thread read" in {
//      insert(shard, Path("/test/small2"), 2, 100, 1)
//      val threadCount = 10 
//      val threads = (0.until(threadCount)) map { _ =>
//        new Thread {
//          override def run() {
//            var cnt = 0
//            while(true) {
//              if(cnt % 100 == 0) println("Test " + cnt)
//              val result = executor.execute("token", "count(load(//test/small2))") 
//              result match {
//                case Success(jval) =>
//                case Failure(e) => new RuntimeException("Query result failure")
//              }
//              cnt += 1
//            }
//          }
//        } 
//      }
//      performBatch(10, 2500) { i =>
//          threads.foreach{ _.start }
//          threads.foreach{ _.join }
//      } 
//    }.pendingUntilFixed
    
    "cleanup" in {
      Await.result(shard.actorsStop, timeout) 
      cleanupTempDir(tmpDir)
      success
    }
  }
}
