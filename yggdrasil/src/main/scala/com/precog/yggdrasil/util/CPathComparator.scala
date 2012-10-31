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
package util

import com.precog.yggdrasil.table._
import com.precog.common.json._

import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.{ specialized => spec }

sealed trait MaybeOrdering {
  def toScalazOrdering: scalaz.Ordering
  def toInt: Int
}

case object Lt extends MaybeOrdering {
  val toScalazOrdering = scalaz.Ordering.LT
  val toInt = -1
}
case object Gt extends MaybeOrdering {
  val toScalazOrdering = scalaz.Ordering.GT
  val toInt = 1
}
case object Eq extends MaybeOrdering {
  val toScalazOrdering = scalaz.Ordering.EQ
  val toInt = 0
}
case object NoComp extends MaybeOrdering {
  val toScalazOrdering = scalaz.Ordering.EQ
  val toInt = 0
}

object MaybeOrdering {
  def fromInt(n: Int): MaybeOrdering = if (n < 0) Lt else if (n == 0) Eq else Gt
}

trait CPathComparator {
  def compare(row1: Int, row2: Int, indices: Array[Int]): MaybeOrdering
}

object CPathComparator {
  import ExtraOrders._

  def apply[@spec(Boolean, Long, Double, AnyRef) A,
            @spec(Boolean, Long, Double, AnyRef) B](
      lCol: Int => A, rCol: Int => B)(implicit order: HetOrder[A, B]) = {
    new CPathComparator {
      def compare(r1: Int, r2: Int, i: Array[Int]) =
        MaybeOrdering.fromInt(order.compare(lCol(r1), rCol(r2)))
    }
  }

  def apply(lPath: CPath, lCol: Column, rPath: CPath, rCol: Column): CPathComparator = (lCol, rCol) match {
    case (lCol: BoolColumn, rCol: BoolColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: LongColumn, rCol: LongColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: LongColumn, rCol: DoubleColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: LongColumn, rCol: NumColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: DoubleColumn, rCol: LongColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: DoubleColumn, rCol: DoubleColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: DoubleColumn, rCol: NumColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: NumColumn, rCol: LongColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: NumColumn, rCol: DoubleColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: NumColumn, rCol: NumColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: StrColumn, rCol: StrColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: DateColumn, rCol: DateColumn) => CPathComparator(lCol(_), rCol(_))
    case (lCol: HomogeneousArrayColumn[_], rCol: HomogeneousArrayColumn[_]) =>
      (lCol.leafTpe, rCol.leafTpe) match {
        case (CLong, CLong) => new ArrayCPathComparator[Long, Long](lPath, lCol, rPath, rCol)
        case (CLong, CDouble) => new ArrayCPathComparator[Long, Double](lPath, lCol, rPath, rCol)
        case (CLong, CNum) => new ArrayCPathComparator[Long, BigDecimal](lPath, lCol, rPath, rCol)
        case (CDouble, CLong) => new ArrayCPathComparator[Double, Long](lPath, lCol, rPath, rCol)
        case (CDouble, CDouble) => new ArrayCPathComparator[Double, Double](lPath, lCol, rPath, rCol)
        case (CDouble, CNum) => new ArrayCPathComparator[Double, BigDecimal](lPath, lCol, rPath, rCol)
        case (CNum, CLong) => new ArrayCPathComparator[BigDecimal, Long](lPath, lCol, rPath, rCol)
        case (CNum, CDouble) => new ArrayCPathComparator[BigDecimal, Double](lPath, lCol, rPath, rCol)
        case (CNum, CNum) => new ArrayCPathComparator[BigDecimal, BigDecimal](lPath, lCol, rPath, rCol)
        case (CBoolean, CBoolean) => new ArrayCPathComparator[Boolean, Boolean](lPath, lCol, rPath, rCol)
        case (CString, CString) => new ArrayCPathComparator[String, String](lPath, lCol, rPath, rCol)
        case (CDate, CDate) => new ArrayCPathComparator[DateTime, DateTime](lPath, lCol, rPath, rCol)
        case (tpe1, tpe2) =>
          val ordering = implicitly[scalaz.Order[CType]].apply(lCol.tpe, rCol.tpe).toInt
          new ArrayCPathComparator[Any, Any](lPath, lCol, rPath, rCol)(implicitly, implicitly, new HetOrder[Any, Any] {
            def compare(a: Any, b: Any): Int = ordering
          })
      }
    case (lCol, rCol) =>
      val ordering = MaybeOrdering.fromInt {
        implicitly[scalaz.Order[CType]].apply(lCol.tpe, rCol.tpe).toInt
      }
      new CPathComparator {
        def compare(r1: Int, r2: Int, indices: Array[Int]) = ordering
      }
  }
}

final class ArrayCPathComparator[@spec(Boolean, Long, Double) A, @spec(Boolean, Long, Double) B](
    lPath: CPath, lCol: HomogeneousArrayColumn[_], rPath: CPath, rCol: HomogeneousArrayColumn[_])(implicit
    ma: Manifest[A], mb: Manifest[B], ho: HetOrder[A, B]) extends CPathComparator {

  private def makeMask(path: CPath): Array[Boolean] = {
    val length = path.nodes.foldLeft(0) {
      case (len, CPathIndex(_) | CPathArray) => len + 1
      case (len, _) => len
    }

    val mask = new Array[Boolean](length)
    path.nodes.zipWithIndex foreach {
      case (CPathArray, i) => mask(i) = true
      case _ =>
    }
    mask
  }

  private val lMask: Array[Boolean] = makeMask(lPath)
  private val rMask: Array[Boolean] = makeMask(rPath)

  val aSelector = new ArraySelector[A]
  val bSelector = new ArraySelector[B]

  def compare(r1: Int, r2: Int, indices: Array[Int]): MaybeOrdering = {
    val left = lCol(r1)
    val right = rCol(r2)

    val lPluckable = aSelector.canPluck(left, indices, lMask)
    val rPluckable = bSelector.canPluck(right, indices, rMask)

    if (lPluckable) {
      if (rPluckable) {
        val a = aSelector.pluck(left, indices, lMask)
        val b = bSelector.pluck(right, indices, rMask)
        val cmp = ho.compare(a, b)
        if (cmp < 0) Lt else if (cmp == 0) Eq else Gt
      } else {
        Gt
      }
    } else if (rPluckable) {
      Lt
    } else {
      NoComp
    }
  }
}

/**
 * ArraySelector provides a non-boxing way of accessing the leaf elements in a
 * bunch of nested arrays.
 */
final class ArraySelector[@spec(Boolean, Long, Double) A](implicit m: Manifest[A]) {
  private val am = m.arrayManifest

  def canPluck(a: Array[_], indices: Array[Int], mask: Array[Boolean]): Boolean = {
    var arr: Array[_] = a
    var i = 0
    while (i < mask.length) {
      if (mask(i)) {
        if (am.erasure.isInstance(arr)) {
          return indices(i) < arr.length
        } else {
          if (indices(i) < arr.length) {
            arr = arr(indices(i)).asInstanceOf[Array[_]]
          } else {
            return false
          }
        }
      }

      i += 1
    }

    return true
  }

  def pluck(a: Array[_], indices: Array[Int], mask: Array[Boolean]): A = {
    var arr: Array[_] = a
    var i = 0

    while (i < mask.length) {
      if (mask(i)) {
        if (am.erasure.isInstance(arr)) {
          val sarr = arr.asInstanceOf[Array[A]]
          return sarr(indices(i))
        } else {
          arr = arr(indices(i)).asInstanceOf[Array[_]]
        }
      }

      i += 1
    }

    sys.error("This shouldn't happens and indicates a problem with canPluck")
  }
}

