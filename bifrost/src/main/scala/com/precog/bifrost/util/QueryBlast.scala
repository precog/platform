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
/**
 * Copyright 2012, ReportGrid, Inc.
 *
 * Created by dchenbecker on 1/15/12 at 7:25 AM
 */
package com.precog.shard.util 

import akka.dispatch.{Future, Await, ExecutionContext}
import akka.util.duration._

import scalaz._

import com.precog.common.Path
import com.precog.common.JValueByteChunkTranscoders._
import com.precog.util.JsonUtil

import blueeyes.bkka._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes.OK
import blueeyes.core.http.MimeTypes._
import blueeyes.core.data._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb

import DefaultBijections._

import blueeyes.json.{JObject, JValue, JString}

import java.lang.{Thread, Object}

import java.util.concurrent.ArrayBlockingQueue
import java.util.Date
import java.util.Properties

import java.io.File
import java.io.FileReader
import java.nio.ByteBuffer

object QueryBlast extends AkkaDefaults {
  var count = 0
  var errors = 0
  var startTime = 0L

  protected implicit val M = new FutureMonad(defaultFutureDispatch)

  var stats = Map[Int, Stats]()
  
  var interval = 10 
  var intervalDouble = interval.toDouble
  
  val notifyLock = new Object

  var maxCount : Option[Int] = None

  class Stats(var count: Int, var errors: Int, var sum: Long, var min: Long, var max: Long)

  def notifyError(index: Int) {
    notifyLock.synchronized {
      errors += 1
      stats.get(index) match {
        case Some(stats) => stats.errors += 1
        case None        => stats = stats + (index -> new Stats(0, 1, 0, 0, 0))
      }
    }
  }

  def notifyComplete(index: Int, nanos : Long) {
    notifyLock.synchronized {
      stats.get(index) match {
        case Some(stats) =>
          stats.count += 1
          stats.sum += nanos
          stats.min = math.min(stats.min, nanos)
          stats.max = math.max(stats.max, nanos)
        case None        => stats = stats + (index -> new Stats(1, 0, nanos, nanos, nanos))
      }
      count += 1
      if ((count + errors) % interval == 0) {
        val now = System.currentTimeMillis()
        stats foreach {
          case (key, stats) =>
            println("%-20d\t%12d\t%f\t%f\t%f\t%f\t(%d)".format(now, stats.errors, intervalDouble / ((now - startTime) / 1000.0d), stats.min / 1000000.0d, stats.max / 1000000.0d, (stats.sum / stats.count) / 1000000.0d, key))
        }
        startTime = now
        stats = Map[Int, Stats]()
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
    val apiKey = properties.getProperty("token", "root")
    val base = properties.getProperty("queryBase", "public")
    interval = properties.getProperty("iterations", "10").toInt
    intervalDouble = interval.toDouble
    val verboseErrors = properties.getProperty("verboseErrors", "false").toBoolean

    val workQueue = new ArrayBlockingQueue[(Int, String)](1000)

    (1 to threads).foreach { id =>
      new Thread {
        val path = "/benchmark/" + id

        override def run() {
          val client = new HttpClientXLightWeb
          while (true) {
            val (index, query) = workQueue.take()
            try {
              val started = System.nanoTime()
             
              val f: Future[HttpResponse[JValue]] = client.path(apiUrl)
                .query("apiKey", apiKey)
                .query("q", query)
                .contentType[ByteChunk](application/MimeTypes.json)
                .get[JValue]("")

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
              notifyComplete(index, System.nanoTime() - started)
            } catch {
              case e =>
                if(verboseErrors) {
                  println("QUERY - ERROR")
                  println("URL: " + apiUrl + "?apiKey="+apiKey)
                  println("QUERY: " + query)
                  println()
                  println("ERROR MESSAGE")
                  e.printStackTrace
                  println()
                }
                notifyError(index)
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
count(load(//%scampaigns))
""",
"""
tests := load(//%scampaigns)
count(tests where tests.gender = "male")
""",
"""
tests := load(//%scampaigns)
histogram('platform) :=
   { platform: 'platform, num: count(tests where tests.platform = 'platform) }
   histogram
"""
  )

  val testQueries = allQueries 

  private val random = new java.util.Random

  def next(base: String, maxQuery: Int): (Int, String) = {
    val index = random.nextInt(maxQuery)
    (index, testQueries(index).format(base).replace("\n", " ").replace("  "," ").trim())
  }
}
