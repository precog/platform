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
package com.reportgrid.storage.leveldb

import org.scalacheck.Arbitrary
import akka.actor.Actor
import akka.dispatch.Future
import Actor._

import java.math._

import java.util.concurrent.{CyclicBarrier,LinkedBlockingQueue}
import java.util.Random


object ContinuousSpeedTest {
  def main (argv : Array[String]) {
    val (actorCount, chunkSize, basedir, seed, dataType) = argv match {
      case Array(ac,tc,bd,s,dtype) => (ac.toInt, tc.toInt, bd, s.toLong, dtype)
      case _ => {
        println("Usage: MultiSpeedTest <number of DBs> <chunk size> <base dir> <random seed>")
        sys.exit(1)
      }
    }
    
    val r = new java.util.Random(seed)

    class TestHarness[T](dbGen : (String) => Column[T], valGen : => T) extends Runnable {
      sealed trait DBOp
      case class Insert(id : Long, value : T) extends DBOp
      case object Flush extends DBOp
    
      def run() {
        var startTime = System.currentTimeMillis
        var id : Long = 0
    
        val barrier = new CyclicBarrier(actorCount + 1, new Runnable {
          def run {
            val duration = System.currentTimeMillis - startTime
    
            println("%d\t%d\t%d\t%f".format(actorCount, id, duration, chunkSize / (duration / 1000.0)))
            startTime = System.currentTimeMillis
          }
        })
    
        // Spin up some processors
        class DBActor(name : String) extends Thread {
          private val column = dbGen(name)
    
          val queue = new java.util.concurrent.LinkedBlockingQueue[DBOp]()
          
          override def run {
            while (true) {
              queue.take match {
                case Insert(id,v) => column.insert(id,v);
                case Flush => barrier.await()
              }
            }
          }
        }
    
        val processors = (1 to actorCount).map { actorId => (new DBActor("speed" + actorId)) }.toArray
    
        processors.foreach(_.start())
    
        // Just keep on inserting
        while (true) {
          var index = 0
    
          while (index < chunkSize) {
            processors(r.nextInt(actorCount)).queue.put(Insert(id, valGen))
            index += 1
            id += 1
          }
    
          processors.foreach{ p => p.queue.put(Flush) }
          barrier.await()
        }
      }
    }

    (dataType.toLowerCase match {
      case "long" => new TestHarness[Long](new Column(_,basedir), r.nextLong)
      case "decimal" => new TestHarness[BigDecimal](new Column(_,basedir), BigDecimal.valueOf(r.nextDouble))
    }).run()
  }
}

// vim: set ts=4 sw=4 et:
