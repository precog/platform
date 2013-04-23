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

import cf._

import blueeyes.json._
import org.joda.time.{DateTime, Period}

import java.math.MathContext

import com.precog.common._
import com.precog.yggdrasil.table._
import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import scala.{ specialized => spec }
import scala.annotation.tailrec

import spire.math.Order

import scalaz.Semigroup
import scalaz.std.option._
import scalaz.syntax.apply._

sealed trait Column {
  def isDefinedAt(row: Int): Boolean
  def |> (f1: CF1): Option[Column] = f1(this)

  val tpe: CType
  def jValue(row: Int): JValue
  def cValue(row: Int): CValue
  def strValue(row: Int): String
  
  def toString(row: Int): String = if (isDefinedAt(row)) strValue(row) else "(undefined)"
  def toString(range: Range): String = range.map(toString(_: Int)).mkString("(", ",", ")")

  def definedAt(from: Int, to: Int): BitSet =
    BitSetUtil.filteredRange(from, to)(isDefinedAt)

  def rowEq(row1: Int, row2: Int): Boolean
  def rowCompare(row1: Int, row2: Int): Int
}

private[yggdrasil] trait ExtensibleColumn extends Column // TODO: or should we just unseal Column?

trait HomogeneousArrayColumn[@spec(Boolean, Long, Double) A] extends Column with (Int => Array[A]) { self =>
  def apply(row: Int): Array[A]

  def rowEq(row1: Int, row2: Int): Boolean = {
    if (!isDefinedAt(row1)) return !isDefinedAt(row2)
    if (!isDefinedAt(row2)) return false

    val a1 = apply(row1)
    val a2 = apply(row2)
    if (a1.length != a2.length) return false
    val n = a1.length
    var i = 0
    while (i < n) {
      if (a1(i) != a2(i)) return false
      i += 1
    }
    true
  }

  def rowCompare(row1: Int, row2: Int): Int = {
    sys.error("...")
  }

  val tpe: CArrayType[A]

  def leafTpe: CValueType[_] = {
    @tailrec def loop(a: CValueType[_]): CValueType[_] = a match {
      case CArrayType(elemType) => loop(elemType)
      case vType => vType
    }

    loop(tpe)
  }

  override def jValue(row: Int) = tpe.jValueFor(this(row))
  override def cValue(row: Int) = tpe(this(row))
  override def strValue(row: Int) = this(row) mkString ("[", ",", "]")

  /**
   * Returns a new Column that selects the `i`-th element from the
   * underlying arrays.
   */
  def select(i: Int) = HomogeneousArrayColumn.select(this, i)
}

object HomogeneousArrayColumn {
  def unapply[A](col: HomogeneousArrayColumn[A]): Option[CValueType[A]] = Some(col.tpe.elemType)

  @inline
  private[table] def select(col: HomogeneousArrayColumn[_], i: Int) = col match {
    case col @ HomogeneousArrayColumn(CString) => new StrColumn {
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): String = col(row)(i)
    }
    case col @ HomogeneousArrayColumn(CBoolean) => new BoolColumn {
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): Boolean = col(row)(i)
    }
    case col @ HomogeneousArrayColumn(CLong) => new LongColumn {
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): Long = col(row)(i)
    }
    case col @ HomogeneousArrayColumn(CDouble) => new DoubleColumn {
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): Double = col(row)(i)
    }
    case col @ HomogeneousArrayColumn(CNum) => new NumColumn {
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): BigDecimal = col(row)(i)
    }
    case col @ HomogeneousArrayColumn(CDate) => new DateColumn {
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): DateTime = col(row)(i)
    }
    case col @ HomogeneousArrayColumn(CPeriod) => new PeriodColumn {
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): Period = col(row)(i)
    }
    case col @ HomogeneousArrayColumn(cType: CArrayType[a]) => new HomogeneousArrayColumn[a] {
      val tpe = cType
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): Array[a] = col(row)(i)
    }
  }
}

trait BoolColumn extends Column with (Int => Boolean) {
  def apply(row: Int): Boolean
  def rowEq(row1: Int, row2: Int): Boolean = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int =
    backport.lang.Boolean.compare(apply(row1), apply(row2))
  
  def asBitSet(undefinedVal: Boolean, size: Int): BitSet = {
    val back = new BitSet(size)
    var i = 0
    while (i < size) {
      val b = if (!isDefinedAt(i))
        undefinedVal
      else
        apply(i)
      
      back.set(i, b)
      i += 1
    }
    back
  }

  override val tpe = CBoolean
  override def jValue(row: Int) = JBool(this(row))
  override def cValue(row: Int) = CBoolean(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString = "BoolColumn"
}

object BoolColumn {
  def True(definedAt: BitSet) = new BitsetColumn(definedAt) with BoolColumn {
    def apply(row: Int) = true
  }

  def False(definedAt: BitSet) = new BitsetColumn(definedAt) with BoolColumn {
    def apply(row: Int) = false
  }

  def Either(definedAt: BitSet, values: BitSet) = new BitsetColumn(definedAt) with BoolColumn {
    def apply(row: Int) = values(row)
  }
}

trait LongColumn extends Column with (Int => Long) {
  def apply(row: Int): Long
  def rowEq(row1: Int, row2: Int): Boolean = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int =
    backport.lang.Long.compare(apply(row1), apply(row2))

  override val tpe = CLong
  override def jValue(row: Int) = JNum(this(row))
  override def cValue(row: Int) = CLong(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString = "LongColumn"
}

trait DoubleColumn extends Column with (Int => Double) {
  def apply(row: Int): Double
  def rowEq(row1: Int, row2: Int): Boolean = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int =
    java.lang.Double.compare(apply(row1), apply(row2))

  override val tpe = CDouble
  override def jValue(row: Int) = JNum(this(row))
  override def cValue(row: Int) = CDouble(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString = "DoubleColumn"
}

trait NumColumn extends Column with (Int => BigDecimal) {
  def apply(row: Int): BigDecimal
  def rowEq(row1: Int, row2: Int): Boolean = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int =
    apply(row1) compare apply(row2)

  override val tpe = CNum
  override def jValue(row: Int) = JNum(this(row))
  override def cValue(row: Int) = CNum(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString = "NumColumn"
}

trait StrColumn extends Column with (Int => String) {
  def apply(row: Int): String
  def rowEq(row1: Int, row2: Int): Boolean = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int =
    apply(row1) compareTo apply(row2)

  override val tpe = CString
  override def jValue(row: Int) = JString(this(row))
  override def cValue(row: Int) = CString(this(row))
  override def strValue(row: Int): String = this(row)
  override def toString = "StrColumn"
}

trait DateColumn extends Column with (Int => DateTime) {
  def apply(row: Int): DateTime
  def rowEq(row1: Int, row2: Int): Boolean = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int =
    apply(row1) compareTo apply(row2)

  override val tpe = CDate
  override def jValue(row: Int) = JString(this(row).toString)
  override def cValue(row: Int) = CDate(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString = "DateColumn"
}

trait PeriodColumn extends Column with (Int => Period) {
  def apply(row: Int): Period
  def rowEq(row1: Int, row2: Int): Boolean = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare periods.")

  override val tpe = CPeriod
  override def jValue(row: Int) = JString(this(row).toString)
  override def cValue(row: Int) = CPeriod(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString = "PeriodColumn"
}

trait EmptyArrayColumn extends Column {
  def rowEq(row1: Int, row2: Int): Boolean = true
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe = CEmptyArray
  override def jValue(row: Int) = JArray(Nil)
  override def cValue(row: Int) = CEmptyArray
  override def strValue(row: Int): String = "[]"
  override def toString = "EmptyArrayColumn"
}
object EmptyArrayColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyArrayColumn
}

trait EmptyObjectColumn extends Column {
  def rowEq(row1: Int, row2: Int): Boolean = true
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe = CEmptyObject
  override def jValue(row: Int) = JObject(Nil)
  override def cValue(row: Int) = CEmptyObject
  override def strValue(row: Int): String = "{}"
  override def toString = "EmptyObjectColumn"
}

object EmptyObjectColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyObjectColumn
}

trait NullColumn extends Column {
  def rowEq(row1: Int, row2: Int): Boolean = true
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe = CNull
  override def jValue(row: Int) = JNull
  override def cValue(row: Int) = CNull
  override def strValue(row: Int): String = "null"
  override def toString = "NullColumn"
}
object NullColumn {
  def apply(definedAt: BitSet) = {
    new BitsetColumn(definedAt) with NullColumn
  }
}

object UndefinedColumn {
  def apply(col: Column) = new Column {
    def rowEq(row1: Int, row2: Int): Boolean = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare undefined values.")
    def isDefinedAt(row: Int) = false
    val tpe = col.tpe
    def jValue(row: Int) = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def cValue(row: Int) = CUndefined
    def strValue(row: Int) = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
  }

  val raw = new Column {
    def rowEq(row1: Int, row2: Int): Boolean = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare undefined values.")
    def isDefinedAt(row: Int) = false
    val tpe = CUndefined
    def jValue(row: Int) = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def cValue(row: Int) = CUndefined
    def strValue(row: Int) = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
  }
}

case class MmixPrng(_seed: Long) {
  private var seed: Long = _seed

  def nextLong(): Long = {
    val next: Long = 6364136223846793005L * seed + 1442695040888963407L
    seed = next
    next
  }

  def nextDouble(): Double = {
    val n = nextLong()
    (n >>> 11) * 1.1102230246251565e-16
  }
}

object Column {
  def rowOrder(col: Column): Order[Int] = new Order[Int] {
    def compare(i: Int, j: Int): Int = {
      if (col.isDefinedAt(i)) {
        if (col.isDefinedAt(j)) {
          col.rowCompare(i, j)
        } else 1
      } else if (col.isDefinedAt(j)) -1 else 0
    }

    def eqv(i: Int, j: Int): Boolean = {
      if (col.isDefinedAt(i)) {
        if (col.isDefinedAt(j)) col.rowEq(i, j) else false
      } else !col.isDefinedAt(j)
    }
  }
  @inline def const(cv: CValue): Column = cv match {
    case CBoolean(v)  => const(v)
    case CLong(v)     => const(v)
    case CDouble(v)   => const(v)
    case CNum(v)      => const(v)
    case CString(v)   => const(v)
    case CDate(v)     => const(v)
    case CArray(v, t @ CArrayType(elemType)) => const(v)(elemType)
    case CEmptyObject => new InfiniteColumn with EmptyObjectColumn 
    case CEmptyArray  => new InfiniteColumn with EmptyArrayColumn 
    case CNull        => new InfiniteColumn with NullColumn 
    case CUndefined   => UndefinedColumn.raw
  }

  @inline def uniformDistribution(init: MmixPrng): (Column, MmixPrng) = {
    val col = new InfiniteColumn with DoubleColumn {
      var memo = scala.collection.mutable.ArrayBuffer.empty[Double]

      def apply(row: Int) = {
        val maxRowComputed = memo.length

        if (row < maxRowComputed) {
          memo(row)
        } else {
          var i = maxRowComputed
          var res = 0d

          while (i <= row) {
            res = init.nextDouble()
            memo += res
            i += 1
          }

          res
        }
      }
    }
    (col, init)
  }

  @inline def const(v: Boolean) = new InfiniteColumn with BoolColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: Long) = new InfiniteColumn with LongColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: Double) = new InfiniteColumn with DoubleColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: BigDecimal) = new InfiniteColumn with NumColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: String) = new InfiniteColumn with StrColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: DateTime) = new InfiniteColumn with DateColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: Period) = new InfiniteColumn with PeriodColumn {
    def apply(row: Int) = v
  }

  @inline def const[@spec(Boolean, Long, Double) A: CValueType](v: Array[A]) = new InfiniteColumn with HomogeneousArrayColumn[A] {
    val tpe = CArrayType(CValueType[A])
    def apply(row: Int) = v
  }

  def lift(col: Column): HomogeneousArrayColumn[_] = col match {
    case col: BoolColumn => new HomogeneousArrayColumn[Boolean] {
      val tpe = CArrayType(CBoolean)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Array(col(row))
    }
    case col: LongColumn => new HomogeneousArrayColumn[Long] {
      val tpe = CArrayType(CLong)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Array(col(row))
    }
    case col: DoubleColumn => new HomogeneousArrayColumn[Double] {
      val tpe = CArrayType(CDouble)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Array(col(row))
    }
    case col: NumColumn => new HomogeneousArrayColumn[BigDecimal] {
      val tpe = CArrayType(CNum)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Array(col(row))
    }
    case col: StrColumn => new HomogeneousArrayColumn[String] {
      val tpe = CArrayType(CString)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Array(col(row))
    }
    case col: DateColumn => new HomogeneousArrayColumn[DateTime] {
      val tpe = CArrayType(CDate)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Array(col(row))
    }
    case col: PeriodColumn => new HomogeneousArrayColumn[Period] {
      val tpe = CArrayType(CPeriod)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Array(col(row))
    }
    case col: HomogeneousArrayColumn[a] => new HomogeneousArrayColumn[Array[a]] {
      val tpe = CArrayType(col.tpe)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int): Array[Array[a]] = Array(col(row))(col.tpe.manifest)
    }
    case _ => sys.error("Cannot lift non-value column.")
  }

  object unionRightSemigroup extends Semigroup[Column] {
    def append(c1: Column, c2: => Column): Column = {
      cf.util.UnionRight(c1, c2) getOrElse {
        sys.error("Illgal attempt to merge columns of dissimilar type: " + c1.tpe + "," + c2.tpe)
      }
    }
  }

  def isDefinedAt(cols: Array[Column], row: Int): Boolean = {
    var i = 0
    while (i < cols.length && !cols(i).isDefinedAt(row)) {
      i += 1
    }
    i < cols.length
  }

  def isDefinedAtAll(cols: Array[Column], row: Int): Boolean = {
    if (cols.isEmpty) {
      false
    } else {
      var i = 0
      while (i < cols.length && cols(i).isDefinedAt(row)) {
        i += 1
      }
      i == cols.length
    }
  }
}
  
abstract class ModUnionColumn(table: Array[Column]) extends Column {
  final def isDefinedAt(i: Int) = {
    val c = col(i)
    c != null && c.isDefinedAt(row(i))
  }
  
  final def col(i: Int) = table(i % table.length)
  final def row(i: Int) = i / table.length
}
