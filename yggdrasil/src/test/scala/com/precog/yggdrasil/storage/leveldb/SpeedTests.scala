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

import com.precog.common.{Path,VectorCase}
import com.precog.util.Bijection
import com.precog.yggdrasil._
import com.precog.yggdrasil.leveldb.LevelDBProjection

import org.apache.commons.io.FileUtils

import org.scalacheck.Arbitrary

import scalaz.effect._

import java.io.File
import java.math.BigDecimal
import java.nio.ByteBuffer

import scala.collection.immutable.ListMap

object SpeedTests {
  def main (argv : Array[String]) {
    val (chunks,chunksize) = argv match {
      case Array(c,cs) => (c.toInt,cs.toInt)
      case _ => {
        println("Usage: SpeedTests <count>")
        sys.exit(1)
      }
    }

    val dataDir = File.createTempFile("ColumnSpec", ".db")
    println("Using %s for dbtest".format(dataDir))
    dataDir.delete() // Ugly, but it works

    val colDesc = ColumnDescriptor(Path("/"), ".foo", CInt, Authorities(Set()))
    
    ProjectionDescriptor(ListMap(colDesc -> 0), Seq(colDesc -> ById)) foreach { descriptor =>
      val writeColumn = LevelDBProjection.forDescriptor(dataDir, descriptor) ||| {
        errors => throw errors
      }
  
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
        val toInsert = (0 until chunksize).map{ id =>
          val indexValue = id + offset * chunksize
          (indexValue, Seq(CInt(indexValue)))
        }
  
        time("writes") {
          toInsert foreach { case (id, value) => writeColumn.insert(VectorCase(id), value).unsafePerformIO }
        }
      }
  
      writeColumn.close.unsafePerformIO
  
      val count = chunks * chunksize
      val totalduration = results.map(_._2).sum
      println("Total duration for %d writes = %d ms (%f/s)".format(count, totalduration, count / (totalduration / 1000.0)))
  
      val readColumn = LevelDBProjection.forDescriptor(dataDir, descriptor) ||| {
        errors => throw errors
      }

      // Pull it all back out
      val readCount = 50
      val readResults = (1 to readCount) map { 
        _=> time("reads") {
          val dataset = readColumn.traverseIndex(System.currentTimeMillis + 3600000l) // One hour should be plenty
          val iterator = dataset.iterable.iterator
          iterator foreach { case (id,v) => /* NOOP */ }
        }
      }
  
      FileUtils.deleteDirectory(dataDir)
      
      val totalReadDuration = readResults.map(_._2).sorted.drop(5).dropRight(5).sum
      val avgReadDuration = totalReadDuration / (readCount - 10)
      println("Read avg = %d ms (%f/s)".format(avgReadDuration, count * 1000.0 / avgReadDuration))

      sys.exit(0)
    }
  }
}

// vim: set ts=4 sw=4 et:
