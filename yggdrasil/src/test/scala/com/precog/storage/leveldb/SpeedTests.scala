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
package com.precog.storage
package leveldb

import com.precog.common.VectorCase
import com.precog.util.Bijection
import com.precog.yggdrasil.leveldb.LevelDBProjection

import org.scalacheck.Arbitrary

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer
import scalaz.effect._

object SpeedTests {
  def main (argv : Array[String]) {
    val (chunks,chunksize) = argv match {
      case Array(c,cs) => (c.toInt,cs.toInt)
      case _ => {
        println("Usage: SpeedTests <count>")
        sys.exit(1)
      }
    }

    val column = LevelDBProjection(new File("/tmp/speed"), sys.error("todo") /*ProjectionComparator.BigDecimal*/) ||| {
      errors => for (err <- errors.list) err.printStackTrace
                sys.error("Errors prevented creation of a LevelDB projection.")
    }

    val biGen = Arbitrary.arbitrary[BigInt]
    val intGen = Arbitrary.arbitrary[Int]

    def time[T](name : String)(f : => T) = {
      println("Beginning " + name)
      val start = System.currentTimeMillis
      val result = f
      val duration = System.currentTimeMillis - start
      println("%s took %d ms".format(name, duration))
      (result,duration)
    }

    // Stuff a whole bunch of crap into the datastore
    val results = (0 until chunks).map { offset =>
      val toInsert = (1 to chunksize).map{ id =>
        for {v     <- biGen.sample
             scale <- intGen.sample} 
          yield (id + offset * 100, new java.math.BigDecimal(v.underlying, scale))
      }

      time("writes") {
        toInsert.foldLeft(IO(())) { case (io, Some((id, value))) => io.flatMap(_ => column.insert(VectorCase(id), sys.error("todo") /*value.as[Array[Byte]].as[ByteBuffer]*/)) } unsafePerformIO
      }
    }

    column.close.unsafePerformIO

    val count = chunks * chunksize
    val totalduration = results.map(_._2).sum
    println("Total duration for %d writes = %d ms (%f/s)".format(count, totalduration, count / (totalduration / 1000.0)))

    //val d = new Column("speed", "/tmp")

    // Pull it all back out
    //time("reads on value index") {
    //  toInsert.foreach{ case Some((id,value)) => 
    //    d.getIds(value)
    //    //if (!d.getIds(value).contains(id)) {
    //    //  println("Data for %s is %s".format(value, d.getIds(value)))
    //    //  println("Missing data for " + id)
    //    //}
    //  }
    //}
  }
}

// vim: set ts=4 sw=4 et:
