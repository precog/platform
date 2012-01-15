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

import java.io.File
import java.nio.ByteBuffer

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.matcher.ThrownMessages
import org.specs2.mutable.{BeforeAfter,Specification}
import org.specs2.specification.Scope
import Bijection._

import scalaz.IdT
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
      val testRange = Interval(Some(size / 4l),Some(size / 2l))
      val db = LevelDBProjection(dataDir, Some(ProjectionComparator.Long))
      db.isSuccess must_== true
      db.map { c =>
        import IdT._

        // Setup our PRNG
        val r = new java.util.Random(seed)
    
        // Create a set of values to insert based on the size
        val values = Array.fill(size)(r.nextLong)
        val pairs  = values.zipWithIndex
        val ids    = pairs.map(_._2)

        logger.info("Inserting %d values".format(size))
    
        // Insert values, syncing every 10%
        val inserts = pairs.grouped(size / 10).toList.map { a =>
          val (first, last) = a.splitAt(a.length - 1)
          for { 
            _ <- first.toList.map { case (v, id) => c.insert(id, v.as[ByteBuffer]) }.sequence[IO, Unit]
            _ <- IO { logger.info("  Syncing insert") }
            _ <- last.toList.map  { case (v, id) => c.insert(id, v.as[ByteBuffer], true) }.sequence[IO, Unit]
          } yield ()
        }

        def reportAllPairs(vals: List[(Long,ByteBuffer)]) = IO {
          vals.forall { v => pairs.contains((v._2.as[Long], v._1)) } must_== true
        }

        def reportValues(vals: List[ByteBuffer]) = IO {
          vals.forall { v => values.contains(v.as[Long]) } must_== true
        }

        def reportIds(vals: List[Long]) = IO {
          vals.forall { v => ids.contains(v) } must_== true
        }

        def reportRangePairs(vals: List[(Long,ByteBuffer)]) = IO {
          val toCompare = testRange match {
            case Interval(Some(low), Some(high)) => pairs.filter { case (v,i) => i >= low && i < high }
          }

          vals.forall { v => toCompare.contains((v._2.as[Long], v._1)) } must_== true
        }

        
        val runTest = for {
          _         <- inserts.sequence[IO, Unit]
          allPairs  <- (fold[Unit, (Long, ByteBuffer), ({ type λ[α] = IdT[IO, α] })#λ, List[(Long,ByteBuffer)]](Nil)((a, e) => e :: a) >>== 
                        c.getAllPairs[Unit].apply[IdT, List[(Long,ByteBuffer)]]).run(_ => idTMonadTrans.liftM[IO, List[(Long,ByteBuffer)]](IO(Nil))).value
          pairRange <- (fold[Unit, (Long, ByteBuffer), ({ type λ[α] = IdT[IO, α] })#λ, List[(Long,ByteBuffer)]](Nil)((a, e) => e :: a) >>== 
                        c.getPairsByIdRange[Unit](testRange).apply[IdT, List[(Long,ByteBuffer)]]).run(_ => idTMonadTrans.liftM[IO, List[(Long,ByteBuffer)]](IO(Nil))).value
          allValues <- (fold[Unit, ByteBuffer, ({ type λ[α] = IdT[IO, α] })#λ, List[ByteBuffer]](Nil)((a, e) => e :: a) >>== 
                        c.getAllValues[Unit].apply[IdT, List[ByteBuffer]]).run(_ => idTMonadTrans.liftM[IO, List[ByteBuffer]](IO(Nil))).value
          allIds    <- (fold[Unit, Long, ({ type λ[α] = IdT[IO, α] })#λ, List[Long]](Nil)((a, e) => e :: a) >>== 
                        c.getAllIds[Unit].apply[IdT, List[Long]]).run(_ => idTMonadTrans.liftM[IO, List[Long]](IO(Nil))).value
          _         <- reportAllPairs(allPairs)
          _         <- reportValues(allValues)
          _         <- reportIds(allIds)
          _         <- c.close
        } yield {
          logger.info("Test complete, shutting down")
        }

        runTest.unsafePerformIO
      }
    }
  }
}
