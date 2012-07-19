package com.precog.yggdrasil
package leveldb

import com.precog.common.{Path,VectorCase}
import com.precog.util.Bijection
import com.precog.yggdrasil._
import com.precog.yggdrasil.leveldb.LevelDBProjection

import org.apache.commons.io.FileUtils

import org.scalacheck.Arbitrary

import scalaz.effect._

import java.io.File
import java.nio.ByteBuffer

import scala.collection.immutable.ListMap

import scalaz._
import scalaz.effect.IO 

import org.scalacheck.Arbitrary

object SpeedTests extends LevelDBProjectionFactory {
  def storageLocation(descriptor: ProjectionDescriptor): IO[File] = {
    IO { IOUtils.createTmpDir("columnSpec") }
  }

  def saveDescriptor(descriptor: ProjectionDescriptor): IO[Validation[Throwable, File]] = {
    storageLocation(descriptor) map { Success(_) }
  }

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
      val writeColumn = LevelDBProjection.forDescriptor(dataDir, descriptor).unsafePerformIO
  
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
  
      val readColumn = LevelDBProjection.forDescriptor(dataDir, descriptor).unsafePerformIO

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
