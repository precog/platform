package com.reportgrid.storage.leveldb

import comparators._
import java.io.File
import java.nio.ByteBuffer
import scala.math.Ordering
import scalaz.effect.IO
import scalaz.iteratee.Iteratee._

import Bijection._

import com.weiglewilczek.slf4s.Logging

object ReadTest extends Logging {
  def main (argv : Array[String]) {
    val (column, size, seed) = argv match {
      case Array(name, size, sd) => (Column(new File(name), Some(ColumnComparator.Long)), size.toInt, sd.toLong)
      case _ => {
        println("Usage: ReadTest <column dir> <insertion count> <random seed>")
        sys.exit(1)
      }
    }

    column.fold({ e => e.list.foreach{ t => logger.error(t.getMessage)}}, { c =>
      // Setup our PRNG
      val r = new java.util.Random(seed)
  
      // Create a set of values to insert based on the size
      val values = Array.fill(size)(r.nextLong)
  
      logger.info("Inserting %d values".format(size))
  
      // Insert values, syncing every 10%
      values.grouped(size / 10).foreach { a =>
        val (first,last) = a.splitAt(a.length - 1)
        first.foreach { v => c.insert(v, ByteBuffer.wrap(v.as[Array[Byte]])) }
        logger.info("  Syncing insert")
        last.foreach { v => c.insert(v, ByteBuffer.wrap(v.as[Array[Byte]]), true) }
      }
  
      // Get a set of all values in the column as ByteBuffers
      val allVals = (fold[Unit, ByteBuffer, IO, List[Long]](Nil)((a, e) => e.as[Long] :: a) >>== 
                     c.getAllValues) apply (_ => IO(Nil)) unsafePerformIO
  
      logger.info("Read " + allVals.size + " distinct values")
  
      allVals.foreach {
        v => if (! values.contains(v)) {
          logger.error("Missing input value: " + v)
        }
      }

      logger.info("Completed check")
  
      c.close

      logger.info("Test complete, shutting down")
    })
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
