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

import com.precog.common.VectorCase
import scala.annotation.tailrec
import scalaz.{Identity => _, _}
import scalaz.Scalaz._
import scalaz.Ordering._

case class SliceF1s(ref: CRef, f1s: Set[F1[_, _]])
case class SliceF2s(ref1: CRef, ref2: CRef, f2s: Set[F2[_, _, _]])

trait Slice { source =>
  import Slice._

  def idCount: Int
  def size: Int
  def isEmpty: Boolean = size == 0

  def identities: Seq[Column[Identity]]
  def columns: Map[CMeta, Column[_]]

  def map(meta: CMeta, refId: Long)(f: F1[_, _]): Slice = new Slice {
    val idCount = source.idCount
    val size = source.size

    val identities = source.identities
    val columns = source.columns.get(meta) map { col =>
                    source.columns + (CMeta(CDyn(refId), f.returns) -> (col |> f))
                  } getOrElse {
                    sys.error("No column found in table matching " + meta)
                  }
  }

  def map(cref: CRef, refId: Long)(f: SliceF1s): Slice = {
    f.f1s.foldLeft(source) { (slice, f1) => slice.map(CMeta(cref, f1.accepts), refId)(f1) }
  }

  def map2(m1: CMeta, m2: CMeta, refId: Long)(f: F2[_, _, _]): Slice = new Slice {
    val idCount = source.idCount
    val size = source.size

    val identities = source.identities
    val columns = {
      val cfopt = for {
        c1 <- source.columns.get(m1)
        c2 <- source.columns.get(m2)
      } yield {
        val fl  = m1.ctype.cast2_1(f)
        val flr = m2.ctype.cast2_2(fl)
        flr(m1.ctype.cast0(c1), m2.ctype.cast0(c2))
      }

      cfopt map { cf => 
        source.columns + (CMeta(CDyn(refId), cf.returns) -> cf)
      } getOrElse {
        sys.error("No column(s) found in table matching " + m1 + " and/or " + m2)
      }
    }
  }

  def map2(cref: CRef, refId: Long)(f: SliceF2s): Slice = {
    f.f2s.foldLeft(source) { (slice, f2) => 
      val (a1, a2) = f2.accepts
      slice.map2(CMeta(cref, a1), CMeta(cref, a2), refId)(f2) 
    }
  }


  def filter(fx: (CMeta, F1[_, Boolean])*): Slice = {
    assert(fx forall { case (m, f0) => columns contains m })
    new Slice {
      private lazy val retained: Vector[Int] = {
        val f0x = fx map { case (m, f0) => m.ctype.cast1(f0)(m.ctype.cast0(columns(m))) }
        @tailrec def check(i: Int, acc: Vector[Int]): Vector[Int] = {
          if (i < source.size) check(i + 1, if (f0x.forall(_(i))) acc :+ i else acc)
          else acc
        }

        check(0, Vector())
      }

      val idCount = source.idCount
      lazy val size = retained.size
      lazy val identities = source.identities map { _ remap retained }
      lazy val columns = source.columns mapValues { _ remap retained }
    }
  }

  def sortByIdentities(idx: VectorCase[Int]): Slice = {
    assert(idx.length <= source.idCount)
    new Slice {
      private val sortedIndices: Array[Int] = {
        import java.util.Arrays
        val arr = Array.range(0, source.size)
        val accessors = idx.map(source.identities).toArray
        val comparator = new IntOrder {
          def order(i1: Int, i2: Int) = {
            var i = 0
            var result: Ordering = EQ
            while (i < accessors.length && (result eq EQ)) {
              val f0 = accessors(i)
              result = longInstance.order(f0(i1), f0(i2))
              i += 1
            }
            result
          }
        }

        Slice.qsort(arr, comparator)
        arr
      }
      
      val idCount = source.idCount
      lazy val size = source.size
      lazy val identities = source.identities map { _ remap sortedIndices }
      lazy val columns = source.columns mapValues { _ remap sortedIndices }
    }
  }

  def sortByValues(meta: CMeta*): Slice = {
    assert(meta.length <= source.idCount)
    new Slice {
      private val sortedIndices: Array[Int] = {
        import java.util.Arrays
        val arr = Array.range(0, source.size)
        val accessors = meta.map(m => (m.ctype, source.columns(m))).toArray
        val comparator = new IntOrder {
          def order(i1: Int, i2: Int) = {
            var i = 0
            var result: Ordering = EQ
            while (i < accessors.length && (result eq EQ)) {
              val (ctype, f0) = accessors(i)
              val f0t = ctype.cast0(f0)
              result = ctype.order(f0t(i1), f0t(i2))
              i += 1
            }
            result
          }
        }

        Slice.qsort(arr, comparator)
        arr
      }
      
      val idCount = source.idCount
      lazy val size = source.size
      lazy val identities = source.identities map { _ remap sortedIndices }
      lazy val columns = source.columns mapValues { _ remap sortedIndices }
    }
  }

  def append(other: Slice): Slice = {
    assert(columns.keySet == other.columns.keySet && idCount == other.idCount) 
    new Slice {
      val idCount = source.idCount
      val size = source.size + other.size
      val identities = (source.identities zip other.identities) map {
        case (c1, c2) => new Column[Long] { 
          val returns = CLong
          def isDefinedAt(row: Int) = (row >= 0 && row < source.size) || (row - source.size >= 0 && row - source.size < other.size)
          def apply(row: Int) = if (row < source.size) c1(row) else c2(row - source.size)
        }
      }

      val columns = other.columns.foldLeft(source.columns) {
        case (acc, (cmeta, col)) => 
          val ctype = cmeta.ctype
          val c1 = ctype.cast0(acc(cmeta))
          val c2 = ctype.cast0(col)
          acc + (
            cmeta -> {
              new Column[ctype.CA] { 
                val returns: CType { type CA = ctype.CA } = ctype
                def isDefinedAt(row: Int) = (row >= 0 && row < source.size) || (row - source.size >= 0 && row - source.size < other.size)
                def apply(row: Int) = if (row < source.size) c1(row) else c2(row - source.size)
              }
            }
          )
      }
    }
  }
}

class ArraySlice(val size: Int, idsData: VectorCase[Array[Long]], data: Map[CMeta, Object]) extends Slice {
  val idCount = idsData.length
  val identities = idsData map { Column.forArray(CLong, _) }
  val columns: Map[CMeta, Column[_]] = data map { case (m @ CMeta(_, ctype), arr) => m -> Column.forArray(ctype, arr.asInstanceOf[Array[ctype.CA]]) } toMap
}

object Slice {
  // scalaz order isn't @specialized
  trait IntOrder {
    def order(i1: Int, i2: Int): Ordering
  }

  private val MIN_QSORT_SIZE = 7; 

  def qsort(x: Array[Int], ord: IntOrder): Unit = {
    val random = new java.util.Random();
    qsortPartial(x, 0, x.length-1, ord, random);
    isort(x, ord);
  }

  private def isort(x: Array[Int], ord: IntOrder): Unit = {
    @tailrec def sort(i: Int): Unit = if (i < x.length) {
      val t = x(i);
      var j = i;
      while(j > 0 && (ord.order(t, x(j-1)) eq LT)) { x(j) = x(j-1); j -= 1 } 
      x(j) = t;
      sort(i + 1)
    }

    sort(0)
  }

  private def qsortPartial(x: Array[Int], lower: Int, upper: Int, ord: IntOrder, random: java.util.Random): Unit = {
    if (upper - lower >= MIN_QSORT_SIZE) {
      swap(x, lower, lower + random.nextInt(upper-lower+1));
      val t = x(lower);
      var i = lower;
      var j = upper + 1;
      var cont = true
      while (cont) {
        do { i += 1 } while (i <= upper && (ord.order(x(i), t) eq LT))
        do { j -= 1 } while (ord.order(t, x(j)) eq LT)
        if (i > j) cont = false
        swap(x, i, j)
      }
    }
  }

  @inline private def swap(xs: Array[Int], i: Int, j: Int) {
    val temp = xs(i);
    xs(i) = xs(j);
    xs(j) = temp;
  }
}
