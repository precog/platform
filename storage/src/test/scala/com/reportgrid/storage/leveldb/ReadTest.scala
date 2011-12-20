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

import java.io.File
import java.nio.ByteBuffer
import scala.math.Ordering
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.effect.IO
import scalaz.iteratee.Iteratee._

import Bijection._

import com.weiglewilczek.slf4s.Logging

object ReadTest extends Logging {
  def main (argv : Array[String]) {
    val (column, size, seed) = argv match {
      case Array(name, size, sd) => (LevelDBProjection(new File(name), Some(ProjectionComparator.Long)), size.toInt, sd.toLong)
      case _ => {
        println("Usage: ReadTest <column dir> <insertion count> <random seed>")
        sys.exit(1)
      }
    }

    column.fold(
      e => e.list.foreach{ t => logger.error(t.getMessage)}, //if unable to construct a column
      c => {
        // Setup our PRNG
        val r = new java.util.Random(seed)
    
        // Create a set of values to insert based on the size
        val values = Array.fill(size)(r.nextLong)
    
        logger.info("Inserting %d values".format(size))
    
        // Insert values, syncing every 10%
        val inserts = values.grouped(size / 10).toList.map { a =>
          val (first, last) = a.splitAt(a.length - 1)
          for { 
            _ <- first.toList.map(v => c.insert(v, ByteBuffer.wrap(v.as[Array[Byte]]))).sequence[IO, Unit]
            _ <- IO { logger.info("  Syncing insert")} 
            _ <- last.toList.map(v => c.insert(v, ByteBuffer.wrap(v.as[Array[Byte]]), true)).sequence[IO, Unit]
          } yield ()
        }

        def report(vals: List[Long]) = IO {
          logger.info("Read " + vals.size + " distinct values")
      
          vals.foreach {
            v => if (! values.contains(v)) logger.error("Missing input value: " + v)
          }

          logger.info("Completed check")
        }

        val runTest = for {
          _       <- inserts.sequence[IO, Unit]
          allVals <- (fold[Unit, ByteBuffer, IO, List[Long]](Nil)((a, e) => e.as[Long] :: a) >>== c.getAllValues) apply (_ => IO(Nil))
          _       <- report(allVals)
          _       <- c.close
        } yield {
          logger.info("Test complete, shutting down")
        }

        runTest.unsafePerformIO
      }
    )
  }
}

/*
object ConfirmTest {
  def main (args : Array[String]) {
    args match {
      case Array(name,base) => {
        val c = new Column(new File(base, name), ColumnComparator.BigDecimal)
        val bd = BigDecimal("123.45")

        List(12l,15l,45345435l,2423423l).foreach(c.insert(_, bd.underlying))

        println(c.getIds(bd.underlying).toList)
      }
    }
  }
}
*/
