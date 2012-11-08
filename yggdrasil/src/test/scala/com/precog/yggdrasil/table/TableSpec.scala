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
package com.precog.yggdrasil
package table

import com.precog.common.VectorCase

import org.specs2.mutable._
import org.specs2.matcher.MatchResult
import scalaz._
import scalaz.std.option
import scalaz.syntax.bind._
import scalaz.syntax.std.option._
import scalaz.Either3._

import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary

object TableSpec extends Specification with ArbitraryProjectionDescriptor with ArbitrarySlice {
  "a table" should {
    "cogroup" in {
      todo
      /*
      "a static full dataset" >> {
        val r1 = ColumnRef(DynColumnId(0), CLong)
        val v1 = new Table(
          1, Set(r1),
          List(
            ArraySlice(
              VectorCase(Array(0L, 1L, 3L, 3L, 5L, 7L, 8L, 8L)),
              Map(r1 -> Array(0L, 1L, 3L, 3L, 5L, 7L, 8L, 8L))
            )
          )
        )

        val r2 = VColumnRef(DynColumnId(1), CLong)
        val v2 = new Table(
          1, Set(r2), 
          List(
            ArraySlice(
              VectorCase(Array(0L, 2L, 3L, 4L, 5L, 5L, 6L, 8L, 8L)),
              Map(r2 -> Array(0L, 2L, 3L, 4L, 5L, 5L, 6L, 8L, 8L))
            )
          )
        )

        val expected = Vector(
          middle3((0L, 0L)),
          left3(1L),
          right3(2L),
          middle3((3L, 3L)),
          middle3((3L, 3L)),
          right3(4L),
          middle3((5L, 5L)),
          middle3((5L, 5L)),
          right3(6L),
          left3(7L),
          middle3((8L, 8L)),
          middle3((8L, 8L)),
          middle3((8L, 8L)),
          middle3((8L, 8L)) 
        )

        val results = v1.cogroup(v2, 1)(CogroupMerge.second)
        val slice = results.slices.iterator.next

        expected.zipWithIndex.foldLeft(ok: MatchResult[Any]) {
          case (result, (Left3(v), i)) =>
            result and 
            (slice.column(VColumnRef(DynColumnId(0), CLong)).get.isDefinedAt(i) must beTrue) and
            (slice.column(VColumnRef(DynColumnId(0), CLong)).get.apply(i) must_== v)

          case (result, (Middle3((l, r)), i)) =>
            result and 
            (slice.column(VColumnRef(DynColumnId(0), CLong)).get.isDefinedAt(i) must beTrue) and
            (slice.column(VColumnRef(DynColumnId(0), CLong)).get.apply(i) must_== l) and
            (slice.column(VColumnRef(DynColumnId(1), CLong)).get.isDefinedAt(i) must beTrue) and
            (slice.column(VColumnRef(DynColumnId(1), CLong)).get.apply(i) must_== r) 

          case (result, (Right3(v), i)) =>
            result and 
            (slice.column(VColumnRef(DynColumnId(1), CLong)).get.isDefinedAt(i) must beTrue) and
            (slice.column(VColumnRef(DynColumnId(1), CLong)).get.apply(i) must_== v) 
        }
      }
      */

      /*
      "perform" in {
        implicit val vm = Validation.validationMonad[String]
        val descriptor = genProjectionDescriptor.sample.get

        val slices1 = listOf(genSlice(descriptor, 10000)).sample.get
        val slices2 = listOf(genSlice(descriptor, 10000)).sample.get

        val table1 = new Table(slices1.head.idCount, slices1.head.columns.keySet, slices1)
        val table2 = new Table(slices2.head.idCount, slices2.head.columns.keySet, slices2)

        val startTime = System.currentTimeMillis
        val resultTable = table1.cogroup(table2)(CogroupMerge.second)

        resultTable.toJson.size
        val elapsed = System.currentTimeMillis - startTime
        println(elapsed)
        elapsed must beGreaterThan(0L)
      }

      "a static single pair dataset" in {
        // Catch bug where equal at the end of input produces middle, right
        val v1s = IterableDataset(1, Vector(rec(0)))
        val v2s = IterableDataset(1, Vector(rec(0)))

        val expected2 = Vector(middle3((0, 0)))

        val results2 = v1s.cogroup(v2s) {
          new CogroupF[Long, Long, Either3[Long, (Long, Long), Long]] {
            def left(i: Long) = left3(i)
            def both(i1: Long, i2: Long) = middle3((i1, i2))
            def right(i: Long) = right3(i)
          }
        }

        Vector(results2.iterator.toSeq: _*) mustEqual expected2
      }

      */
    }
  }
}
