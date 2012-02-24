/**
 * Copyright 2012, ReportGrid, Inc.
 *
 * Created by dchenbecker on 1/15/12 at 7:25 AM
 */
package com.precog.shard.util 

import akka.dispatch.{Future, Await}
import akka.util.duration._

import com.precog.analytics.Path
import com.precog.common.security._

import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpStatusCodes.OK
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data.BijectionsChunkJson._
import blueeyes.core.service.engines.HttpClientXLightWeb

import _root_.blueeyes.json.JsonAST.{JObject, JValue, JString}

import java.lang.{Thread, Object}

import java.util.concurrent.ArrayBlockingQueue
import java.util.Date
import java.util.Properties

import java.io.File
import java.io.FileReader

object QueryBlast {
  var count = 0
  var errors = 0
  var startTime = 0L
  var sum = 0l
  var min = Long.MaxValue
  var max = Long.MinValue
  
  var interval = 10 
  var intervalDouble = interval.toDouble
  
  val notifyLock = new Object

  var maxCount : Option[Int] = None

  def notifyError() {
    notifyLock.synchronized {
      errors += 1
    }
  }

  def notifyComplete(nanos : Long) {
    notifyLock.synchronized {
      count += 1
      sum += nanos
      min = math.min(min, nanos)
      max = math.max(max, nanos)

      if ((count + errors) % interval == 0) {
        val now = System.currentTimeMillis()
        println("%-20d\t%12d\t%f\t%f\t%f\t%f".format(now, errors, intervalDouble / ((now - startTime) / 1000.0d), min / 1000000.0d, max / 1000000.0d, (sum / intervalDouble) / 1000000.0d))
        startTime = now
        sum = 0l
        min = Long.MaxValue
        max = Long.MinValue
      }
    }

    maxCount.foreach { mc => if (count >= mc) { println("Shutdown"); sys.exit() } }
  } 
  
  def main(args: Array[String]) {
    if(args.size == 0) usage() else runTest(loadConfig(args)) 
  }

  def usage() {
    println(usageMessage)
    sys.exit(1)
  }

  val usageMessage = 
""" 
Usage: command {properties file}

Properites:
threads - number of threads for the test (default: 1)
iteration - number of iterations between (default: 10)
baseUrl - base url for test (default: http://localhost:30070/query)
verboseErrors - whether to print verbose error messages (default: false)
"""

  def loadConfig(args: Array[String]): Properties = { 
    if(args.length != 1) usage() 
        
    val config = new Properties()
    val file = new File(args(0))
        
    if(!file.exists) usage() 
        
    config.load(new FileReader(file))
    config
  }

  def runTest(properties: Properties) {
    val sampleSet = new QuerySampler 
    val apiUrl = properties.getProperty("baseUrl", "http://localhost:30070/query")
    val threads = properties.getProperty("threads", "1").toInt 
    val maxQuery = properties.getProperty("maxQuery", sampleSet.testQueries.size.toString).toInt 
    val token = properties.getProperty("token", StaticTokenManager.rootUID)
    val base = properties.getProperty("queryBase", "public")
    interval = properties.getProperty("iterations", "10").toInt
    intervalDouble = interval.toDouble
    val verboseErrors = properties.getProperty("verboseErrors", "false").toBoolean


    val workQueue = new ArrayBlockingQueue[JValue](1000)

//    println("Starting workers")
    
    (1 to threads).foreach { id =>
      new Thread {
        val path = "/benchmark/" + id

        override def run() {
          val client = new HttpClientXLightWeb
          while (true) {
            val sample = workQueue.take()
            try {
              val started = System.nanoTime()
              
              val f: Future[HttpResponse[JValue]] = client.path(apiUrl)
                                                          .query("tokenId", token)
                                                          .contentType(application/MimeTypes.json)
                                                          .post[JValue]("")(sample)

              Await.ready(f, 120 seconds)
              f.value match {
                case Some(Right(HttpResponse(status, _, _, _))) if status.code == OK => ()
                case Some(Right(HttpResponse(status, _, _, _)))                      =>  
                  throw new RuntimeException("Server returned error code with request")
                case Some(Left(ex))                                              =>  
                  throw ex
                case _                                                           =>  
                  throw new RuntimeException("Error processing insert request") 
              }    
              notifyComplete(System.nanoTime() - started)
            } catch {
              case e =>
                if(verboseErrors) {
                  println("QUERY - ERROR")
                  println(sample)
                  println()
                  println("ERROR MESSAGE")
                  e.printStackTrace
                  println()
                }
                notifyError()
            }
          }
        }
      }.start()
    }

    // Start injecting
    startTime = System.currentTimeMillis()
    //println("Starting sample inject")
    println("time                \ttotal errors\tqueries/s\tmin (ms)\tmax (ms)\tavg (ms)")
    while(true) {
      val sample = sampleSet.next(base, maxQuery)
      workQueue.put(sample)
    }
  }
}

class QuerySampler {
  val allQueries = List(
"""
count(dataset(//%s/campaigns))
""",
"""
tests := dataset(//%s/campaigns)
count(tests where tests.gender = "male")
""",
"""
tests := dataset(//%s/campaigns)
histogram('platform) :=
   { platform: 'platform, num: count(tests where tests.platform = 'platform) }
   histogram
"""
  )

  val testQueries = allQueries 

  private val random = new java.util.Random

  def next(base: String, maxQuery: Int): JValue = JString(testQueries(random.nextInt(maxQuery)).format(base))
}
