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
package com.precog.gjallerhorn

import dispatch._

import com.precog.yggdrasil._
import org.scalacheck._

import java.util.concurrent.atomic._

class IngestStress(settings: Settings) extends Task(settings) {
  import SampleData._
  
  val eps = 10000
  val clients = Runtime.getRuntime.availableProcessors
  
  val accountSet = Vector.fill(clients)(createAccount)
  
  def randomIngest(account: Account, n: Int, generator: Arbitrary[SampleData]): Double = {
    def loop(n: Int, accumulated: Int = 0): Int = {
      val sd = generator.arbitrary.sample.get
      val actualSize = sd.data.length
      val str = sd.data map { _.renderCompact } mkString "\n"
      
      try {
        ingestString(account.apiKey, account, str, "application/json")(_ / account.bareRootPath / "foo")
      } catch {
        case t => println(t)
      }
      
      if (n > 0)
        loop(n - 1, accumulated + sd.data.length)
      else
        accumulated + sd.data.length
    }
    
    val start = System.currentTimeMillis
    val total = loop(n / 20)
    (total.toDouble * 1000) / (System.currentTimeMillis - start)
  }
  
  val rates = new AtomicReferenceArray[Double](clients)
  val throttles = new AtomicReferenceArray[Double](clients)
  
  val threads = accountSet.zipWithIndex map {
    case (account, i) => {
      new Thread(new Runnable {
        def run() {
          val generator = sample(schema)
          while (true) {
            val rate = randomIngest(account, eps / clients, generator)
            rates.set(i, rate)
            
            // throttle to the target EPS
            if (rate > eps) {
              val delay = 1000 / (rate - eps)
              throttles.set(i, delay)
              Thread.sleep(delay.toLong)
            }
          }
        }
      })
    }
  }
  
  println("Starting ingestion; batton down the hatches! (Ctrl-C to abort)")
  println("Target EPS: %d; Threads: %d".format(eps, clients))
  
  threads foreach { _.start() }
  
  while (true) {
    Thread.sleep(5000)
    
    val burstRate = math.round(0 until clients map rates.get sum)
    val throttle = math.round(0 until clients map throttles.get sum)
    
    println(">>> burst rate = %s; current throttle = %s".format(burstRate, throttle))
  }
}

object IngestStress {
  def main(args: Array[String]) {
    try {
      val path = args.headOption.getOrElse("bifrost.out")
      val settings = Settings.fromFile(new java.io.File(path))
      new IngestStress(settings)
    } finally {
      Http.shutdown()
    }
  }
}
