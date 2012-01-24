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
package com.reportgrid.storage
package leveldb

import com.reportgrid.util.Bijection
import com.reportgrid.yggdrasil.leveldb.ProjectionComparator
import com.reportgrid.yggdrasil.leveldb.LevelDBProjection

import org.iq80.leveldb._
import org.scalacheck.Arbitrary
import akka.actor.Actor
import akka.dispatch.Future
import Actor._

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.util.concurrent.{CyclicBarrier,LinkedBlockingQueue}
import java.util.Random

import scala.math.Ordering

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

    class TestHarness[T: ({type X[a] = Bijection[a, Array[Byte]]})#X](dbGen : (String) => LevelDBProjection, valGen : => T) extends Runnable {
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
                case Insert(id, v) => column.insert(Vector(id), sys.error("todo")/*v.as[Array[Byte]].as[ByteBuffer]*/);
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

    def column(n: String, comparator: DBComparator): LevelDBProjection = {
      LevelDBProjection(new File(basedir, n), sys.error("todo")/*Some(comparator)*/) ||| { errors =>
        errors.list.foreach(_.printStackTrace); sys.error("Could not obtain column.")
      }
    }
/*
    (dataType.toLowerCase match {
      case "long"    => sys.error("todo") //new TestHarness[Long](n => column(n, ProjectionComparator.Long), r.nextLong)
      case "decimal" => sys.error("todo") // new TestHarness[BigDecimal](n => column(n, ProjectionComparator.BigDecimal), BigDecimal.valueOf(r.nextDouble))
    }).run() */
  }
}
// vim: set ts=4 sw=4 et:
