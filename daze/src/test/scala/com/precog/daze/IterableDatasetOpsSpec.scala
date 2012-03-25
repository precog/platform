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
package com.precog
package daze

import yggdrasil._
import com.precog.common._

import java.io.File
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.anyVal._
import Iteratee._
import Either3._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

class IterableDatasetOpsSpec extends Specification with ScalaCheck with IterableDatasetOpsComponent {
  type YggConfig = SortConfig

  object ops extends Ops
  import ops._

  object yggConfig extends SortConfig {
    def sortBufferSize: Int = 1000
    def sortWorkDir: File = new File("/tmp")
  }

  def rec(i: Long) = (VectorCase(i), i: java.lang.Long)

  type Record[A <: AnyRef] = (Identities, A)

  implicit def genLong = for (l <- arbitrary[Long]) yield (l: java.lang.Long)

  implicit def recGen[A <: AnyRef](implicit agen: Gen[A]): Gen[Record[A]] = 
    for {
      id <- arbitrary[Long]
      value <- agen
    } yield {
      (VectorCase(id), value)
    }

  implicit def dsGen[A <: AnyRef](implicit rgen: Gen[Record[A]]): Gen[IterableDataset[A]] = {
    for (l <- listOf(rgen)) yield IterableDataset(1, l)
  }

  implicit def arbIterableDataset[A <: AnyRef](implicit gen: Gen[Record[A]]): Arbitrary[IterableDataset[A]] =
     Arbitrary(dsGen[A])

  "iterable dataset ops" should {
    "cogroup" in {
      import java.lang.Long
      val v1  = IterableDataset(1, Vector(rec(1), rec(3), rec(3), rec(5), rec(7), rec(8), rec(8)))
      val v2  = IterableDataset(1, Vector(rec(2), rec(3), rec(4), rec(5), rec(5), rec(6), rec(8), rec(8)))

      val expected = Vector(
        left3(1: Long),
        right3(2: Long),
        middle3((3: Long, 3: Long)),
        middle3((3: Long, 3: Long)),
        right3(4: Long),
        middle3((5: Long, 5: Long)),
        middle3((5: Long, 5: Long)),
        right3(6: Long),
        left3(7: Long),
        middle3((8: Long, 8: Long)),
        middle3((8: Long, 8: Long)),
        middle3((8: Long, 8: Long)),
        middle3((8: Long, 8: Long)) 
      )

      val results = v1.cogroup(v2) {
        new CogroupF[java.lang.Long, java.lang.Long, Either3[java.lang.Long, (java.lang.Long, java.lang.Long), java.lang.Long]] {
          def left(i: java.lang.Long) = left3(i)
          def both(i1: java.lang.Long, i2: java.lang.Long) = middle3((i1, i2))
          def right(i: java.lang.Long) = right3(i)
        }
      }

      Vector(results.iterator.toSeq: _*) must_== expected
    }

    "join" in {
      val v1  = IterableDataset(1, Vector(rec(1), rec(3), rec(3), rec(5), rec(7), rec(8), rec(8)))
      val v2  = IterableDataset(1, Vector(rec(2), rec(3), rec(4), rec(5), rec(5), rec(6), rec(8), rec(8)))
      
      val expected = Vector(
        (3, 3),
        (3, 3),
        (5, 5),
        (5, 5),
        (8, 8),
        (8, 8),
        (8, 8),
        (8, 8) 
      )

      val results = v1.join(v2, 1) {
        case (a, b) => (a, b)
      }

      Vector(results.iterator.toSeq: _*) must_== expected
    }

    "crossLeft" in {
      check { (l1: IterableDataset[java.lang.Long], l2: IterableDataset[java.lang.Long]) => 
        val results = l1.crossLeft(l2) { 
          case (a, b) => (a, b)
        }

        results.iterator.toList must_== l1.flatMap(i => l2.map((j: java.lang.Long) => (i, j)))
      } 
    }

    "crossRight" in {
      check { (l1: IterableDataset[java.lang.Long], l2: IterableDataset[java.lang.Long]) => 
        val results = l1.crossRight(l2) { 
          case (a, b) => (a, b)
        }

        results.iterator.toList must_== l2.flatMap(i => l1.map((j: java.lang.Long) => (j, i)))
      } 
    }
  }
}
