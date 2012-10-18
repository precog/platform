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

import blueeyes.json.JsonAST._
import org.joda.time.DateTime

import java.math.MathContext

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

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
}

private[yggdrasil] trait ExtensibleColumn extends Column // TODO: or should we just unseal Column?

trait BoolColumn extends Column with (Int => Boolean) {
  def apply(row: Int): Boolean

  override val tpe = CBoolean
  override def jValue(row: Int) = JBool(this(row))
  override def cValue(row: Int) = CBoolean(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString = "BoolColumn"
}

trait LongColumn extends Column with (Int => Long) {
  def apply(row: Int): Long

  override val tpe = CLong
  override def jValue(row: Int) = JNum(BigDecimal(this(row), MathContext.UNLIMITED))
  override def cValue(row: Int) = CLong(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString = "LongColumn"
}

trait DoubleColumn extends Column with (Int => Double) {
  def apply(row: Int): Double

  override val tpe = CDouble
  override def jValue(row: Int) = JNum(BigDecimal(this(row).toString, MathContext.UNLIMITED))
  override def cValue(row: Int) = CDouble(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString = "DoubleColumn"
}

trait NumColumn extends Column with (Int => BigDecimal) {
  def apply(row: Int): BigDecimal

  override val tpe = CNum
  override def jValue(row: Int) = JNum(this(row))
  override def cValue(row: Int) = CNum(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString = "NumColumn"
}

trait StrColumn extends Column with (Int => String) {
  def apply(row: Int): String

  override val tpe = CString
  override def jValue(row: Int) = JString(this(row))
  override def cValue(row: Int) = CString(this(row))
  override def strValue(row: Int): String = this(row)
  override def toString = "StrColumn"
}

trait DateColumn extends Column with (Int => DateTime) {
  def apply(row: Int): DateTime

  override val tpe = CDate
  override def jValue(row: Int) = JString(this(row).toString)
  override def cValue(row: Int) = CDate(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString = "DateColumn"
}


trait EmptyArrayColumn extends Column {
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
    def isDefinedAt(row: Int) = false
    val tpe = col.tpe
    def jValue(row: Int) = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def cValue(row: Int) = CUndefined
    def strValue(row: Int) = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
  }

  val raw = new Column {
    def isDefinedAt(row: Int) = false
    val tpe = CUndefined
    def jValue(row: Int) = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def cValue(row: Int) = CUndefined
    def strValue(row: Int) = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
  }
}

object Column {
  @inline def const(cv: CValue): Column = cv match {
    case CBoolean(v)  => const(v)
    case CLong(v)     => const(v)
    case CDouble(v)   => const(v)
    case CNum(v)      => const(v)
    case CString(v)   => const(v)
    case CDate(v)     => const(v)
    case CEmptyObject => new InfiniteColumn with EmptyObjectColumn 
    case CEmptyArray  => new InfiniteColumn with EmptyArrayColumn 
    case CNull        => new InfiniteColumn with NullColumn 
    case CUndefined   => UndefinedColumn.raw
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
}
