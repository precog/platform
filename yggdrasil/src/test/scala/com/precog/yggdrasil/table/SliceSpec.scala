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
package yggdrasil
package table

import scala.util.Random

import com.precog.common._
import com.precog.common.json._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._

class SliceSpec extends Specification with ArbitrarySlice with ScalaCheck {

  implicit def cValueOrdering: Ordering[CValue] = CValue.CValueOrder.toScalaOrdering
  implicit def listOrdering[A](implicit ord0: Ordering[A]) = new Ordering[List[A]] {
    def compare(a: List[A], b: List[A]): Int =
      (a zip b) map ((ord0.compare _).tupled) find (_ != 0) getOrElse (a.length - b.length)
  }

  def extractCValues(colGroups: List[List[Column]], row: Int): List[CValue] = {
    colGroups map { _ find (_.isDefinedAt(row)) map (_.cValue(row)) getOrElse CUndefined }
  }

  def columnsByCPath(slice: Slice): Map[CPath, List[Column]] = {
    val byCPath = slice.columns.groupBy(_._1.selector)
    byCPath.mapValues(_.map(_._2).toList)
  }

  def sortableCValues(slice: Slice, cpaths: VectorCase[CPath]): List[(List[CValue], List[CValue])] = {
    val byCPath = columnsByCPath(slice)
    (0 until slice.size).map({ row =>
      (extractCValues(cpaths.map(byCPath).toList, row), extractCValues(byCPath.values.toList, row))
    })(collection.breakOut)
  }

  def toCValues(slice: Slice) = sortableCValues(slice, VectorCase.empty) map (_._2)

  def fakeSort(slice: Slice, sortKey: VectorCase[CPath]) =
    sortableCValues(slice, sortKey).sortBy(_._1).map(_._2)

  def fakeConcat(slices: List[Slice]) = {
    slices.foldLeft(List.empty[List[CValue]]) { (acc, slice) =>
      acc ++ toCValues(slice)
    }
  }

  def stripUndefineds(cvals: List[CValue]): Set[CValue] =
    (cvals filter (_ != CUndefined)).toSet

  "sortBy" should {
    "sort a trivial slice" in {
      val slice = new Slice {
        val size = 5
        val columns = Map(
          ColumnRef(CPath("a"), CLong) -> new LongColumn {
            def isDefinedAt(row: Int) = true
            def apply(row: Int) = -row.toLong
          },
          ColumnRef(CPath("b"), CLong) -> new LongColumn {
            def isDefinedAt(row: Int) = true
            def apply(row: Int) = row / 2
          })
      }
      val sortKey = VectorCase(CPath("a"))

      fakeSort(slice, sortKey) must_== toCValues(slice.sortBy(sortKey))
    }

    "sort arbitrary slices" in { check { badSize: Int =>
      val path = Path("/")
      val auth = Authorities(Set())
      val paths = Vector(
        CPath("0") -> CLong,
        CPath("1") -> CBoolean,
        CPath("2") -> CString,
        CPath("3") -> CDouble,
        CPath("4") -> CNum,
        CPath("5") -> CEmptyObject,
        CPath("6") -> CEmptyArray,
        CPath("7") -> CNum)
      val pd = ProjectionDescriptor(0, paths.toList map { case (cpath, ctype) =>
        ColumnDescriptor(path, cpath, ctype, auth)
      })

      val size = scala.math.abs(badSize % 100).toInt
      implicit def arbSlice = Arbitrary(genSlice(pd, size))

      check { slice: Slice =>
        for (i <- 0 to 7; j <- 0 to 7) {
          val sortKey = if (i == j) {
            VectorCase(paths(i)._1)
          } else {
            VectorCase(paths(i)._1, paths(j)._1)
          }
          fakeSort(slice, sortKey) must_== toCValues(slice.sortBy(sortKey))
        }
      }
    } }
  }

  private def concatProjDesc = {
    val path = Path("/")
    val auth = Authorities(Set())
    val paths = Vector(
      CPath("0") -> CLong,
      CPath("1") -> CBoolean,
      CPath("2") -> CString,
      CPath("3") -> CDouble,
      CPath("4") -> CNum,
      CPath("5") -> CEmptyObject,
      CPath("6") -> CEmptyArray,
      CPath("7") -> CNum)
    ProjectionDescriptor(0, paths.toList map { case (cpath, ctype) =>
      ColumnDescriptor(path, cpath, ctype, auth)
    })
  }

  "concat" should {
    "concat arbitrary slices together" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 23))

      check { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    "concat small singleton together" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 1))

      check { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    val emptySlice = new Slice {
      val size = 0
      val columns: Map[ColumnRef, Column] = Map.empty
    }

    "concat empty slices correctly" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 23))

      check { fullSlices: List[Slice] =>
        val slices = fullSlices collect {
          case slice if Random.nextBoolean => slice
          case _ => emptySlice
        }
        val slice = Slice.concat(slices)
        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    "concat heterogeneous slices" in {
      def filterColumns(pd: ProjectionDescriptor): ProjectionDescriptor = {
        pd.copy(columns = pd.columns filter (_ => Random.nextBoolean))
      }

      val pds = List.fill(25)(filterColumns(concatProjDesc))
      val g1 :: g2 :: gs = pds.map(genSlice(_, 17))

      implicit val arbSlice = Arbitrary(Gen.oneOf(g1, g2, gs: _*))
      
      check { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        // This is terrible, but there isn't an immediately easy way to test
        // without duplicating concat.
        toCValues(slice).map(stripUndefineds) must_== fakeConcat(slices).map(stripUndefineds)
      }
    }
  }
}

