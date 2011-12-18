package com.reportgrid.storage.leveldb

import comparators._
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
      case Array(name, size, sd) => (Column(new File(name), Some(ColumnComparator.Long)), size.toInt, sd.toLong)
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
