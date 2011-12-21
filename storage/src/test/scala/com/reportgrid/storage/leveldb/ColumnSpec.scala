package com.reportgrid.storage.leveldb

import java.io.File
import java.nio.ByteBuffer

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.matcher.ThrownMessages
import org.specs2.mutable.{BeforeAfter,Specification}
import org.specs2.specification.Scope
import Bijection._

import scalaz.syntax.traverse._ 
import scalaz.std.list._ 
import scalaz.effect.IO 
import scalaz.iteratee.Iteratee._

import com.weiglewilczek.slf4s.Logging

class ColumnSpec extends Specification with ScalaCheck with ThrownMessages with Logging {
  trait columnSetup extends Scope with BeforeAfter {
    val dataDir = File.createTempFile("ColumnSpec", ".db")
    logger.info("Using %s for dbtest".format(dataDir))
    def before = dataDir.delete() // Ugly, but it works
    def after = {
      // Here we need to remove the entire directory and contents
      def delDir (dir : File) {
        dir.listFiles.foreach {
          case d if d.isDirectory => delDir(d)
          case f => f.delete()
        }
        dir.delete()
      }
      delDir(dataDir)
    }
  }

  "Columns" should {
    "Fail to create a new column without a provided comparator" in new columnSetup {
      LevelDBProjection(dataDir).isFailure must_== true
    }

    "Create a new column with a provided comparator" in new columnSetup {
      val c = LevelDBProjection(dataDir, Some(ProjectionComparator.Long))
      c.isSuccess must_== true
      c.map(_.close.unsafePerformIO)
    }

    "Open an existing column with a restored comparator" in new columnSetup {
      val initial = LevelDBProjection(dataDir, Some(ProjectionComparator.Long))
      initial.isSuccess must_== true
      initial.map(_.close.unsafePerformIO).flatMap(_ => LevelDBProjection(dataDir)).isSuccess must_== true
    }

    "Properly persist and restore values" in new columnSetup {
      val size = 1000
      val seed = 42l
      val db = LevelDBProjection(dataDir, Some(ProjectionComparator.Long))
      db.isSuccess must_== true
      db.map { c =>
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
      
          vals.forall {
            v => values.contains(v)
          } must_== true
        }

        val runTest = for {
          _       <- inserts.sequence[IO, Unit]
          allVals <- (fold[Unit, Long, IO, List[Long]](Nil)((a, e) => e :: a) >>== c.getAllIds) apply (_ => IO(Nil))
          _       <- report(allVals)
          _       <- c.close
        } yield {
          logger.info("Test complete, shutting down")
        }

        runTest.unsafePerformIO
      }
    }
  }
}
