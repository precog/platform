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

import comparators._
import java.io.File
import scala.math.Ordering
import scalaz.effect.IO
import scalaz.iteratee.Iteratee._

object ReadTest {
  def main (argv : Array[String]) {
    val (column, chunkSize, seed) = argv match {
      case Array(name, basedir, size, sd) => (new Column(new File(basedir, name), ColumnComparator.Long), size.toInt, sd.toInt)
      case _ => {
        println("Usage: ReadTest <column name> <base dir>")
        sys.exit(1)
      }
    }

    // Setup our PRNG
    val r = new java.util.Random(seed)

    // Get a set of all values in the column
    val allVals = column.getAllValues.toArray

    println("Read " + allVals.size + " distinct values")

    // Outer loop forever
    while (true) {
      var index = 0
      var totalRead = 0
      val startTime = System.currentTimeMillis
      while (index < chunkSize) {
        val toRead = allVals(r.nextInt(allVals.size))
        //val relatedIds : List[Long] = ((fold[Unit, Long, IO, List[Long]](Nil)((a,e) => e :: a) >>== column.getIds(toRead)) apply(_ => IO(Nil)) unsafePerformIO)
        //totalRead += relatedIds.size
        //println(toRead + " => " + relatedIds)
        index += 1
      }
      val duration = System.currentTimeMillis - startTime

      println("%d\t%f".format(totalRead, chunkSize / (duration / 1000.0)))
    }
  }
}

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
