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

import scala.collection.mutable.BitSet
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

  def definedAt(from: Int, to: Int): BitSet = BitSet((for (i <- from until to if isDefinedAt(i)) yield i) : _*)
}

private[yggdrasil] trait ExtensibleColumn extends Column // TODO: or should we just unseal Column?

trait HomogeneousArrayColumn[A] extends Column with (Int => IndexedSeq[A]) { self =>
  def apply(row: Int): IndexedSeq[A]

  val tpe: CArrayType[A]
  override def jValue(row: Int) = tpe.jValueFor(this(row))
  override def cValue(row: Int) = tpe(this(row))
  override def strValue(row: Int) = this(row) mkString ("[", ",", "]")

  /**
   * Returns a new Column that selects the `i`-th element from the
   * underlying arrays.
   */
  def select(i: Int) = HomogeneousArrayColumn.select(this, i)

  /**
   * Returns a view of this column, whose indicies, defined in `removed` have
   * been removed. For example, `col.without(Set(0, 2))`, returns a view of
   * this column, but removes the 1st and 3rd element from the returned
   * arrays.
   */
  def without(removed: Set[Int]) = new HomogeneousArrayColumn[A] {
    def apply(row: Int): IndexedSeq[A] =
      self.apply(row).zipWithIndex filterNot (e => removed(e._2)) map (_._1)
    def isDefinedAt(row: Int) = self.isDefinedAt(row)
    val tpe = self.tpe
  }
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
    case col @ HomogeneousArrayColumn(cType: CArrayType[a]) => new HomogeneousArrayColumn[a] {
      val tpe = cType
      def isDefinedAt(row: Int): Boolean =
        i >= 0 && col.isDefinedAt(row) && i < col(row).length
      def apply(row: Int): IndexedSeq[a] = col(row)(i)
    }
  }
}

trait BoolColumn extends Column with (Int => Boolean) {
  def apply(row: Int): Boolean

  override val tpe = CBoolean
  override def jValue(row: Int) = JBool(this(row))
  override def cValue(row: Int) = CBoolean(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
}

trait LongColumn extends Column with (Int => Long) {
  def apply(row: Int): Long

  override val tpe = CLong
  override def jValue(row: Int) = JNum(BigDecimal(this(row), MathContext.UNLIMITED))
  override def cValue(row: Int) = CLong(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
}

trait DoubleColumn extends Column with (Int => Double) {
  def apply(row: Int): Double

  override val tpe = CDouble
  override def jValue(row: Int) = JNum(BigDecimal(this(row).toString, MathContext.UNLIMITED))
  override def cValue(row: Int) = CDouble(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
}

trait NumColumn extends Column with (Int => BigDecimal) {
  def apply(row: Int): BigDecimal

  override val tpe = CNum
  override def jValue(row: Int) = JNum(this(row))
  override def cValue(row: Int) = CNum(this(row))
  override def strValue(row: Int): String = this(row).toString
}

trait StrColumn extends Column with (Int => String) {
  def apply(row: Int): String

  override val tpe = CString
  override def jValue(row: Int) = JString(this(row))
  override def cValue(row: Int) = CString(this(row))
  override def strValue(row: Int): String = this(row)
}

trait DateColumn extends Column with (Int => DateTime) {
  def apply(row: Int): DateTime

  override val tpe = CDate
  override def jValue(row: Int) = JString(this(row).toString)
  override def cValue(row: Int) = CDate(this(row))
  override def strValue(row: Int): String = this(row).toString
}


trait EmptyArrayColumn extends Column {
  override val tpe = CEmptyArray
  override def jValue(row: Int) = JArray(Nil)
  override def cValue(row: Int) = CEmptyArray
  override def strValue(row: Int): String = "[]"
}
object EmptyArrayColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyArrayColumn
}

trait EmptyObjectColumn extends Column {
  override val tpe = CEmptyObject
  override def jValue(row: Int) = JObject(Nil)
  override def cValue(row: Int) = CEmptyObject
  override def strValue(row: Int): String = "{}"
}

object EmptyObjectColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyObjectColumn
}

trait NullColumn extends Column {
  override val tpe = CNull
  override def jValue(row: Int) = JNull
  override def cValue(row: Int) = CNull
  override def strValue(row: Int): String = "null"
}
object NullColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with NullColumn
}

object UndefinedColumn {
  def apply(col: Column) = new Column {
    def isDefinedAt(row: Int) = false
    val tpe = col.tpe
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
    case CArray(v, CArrayType(elemType)) => const(v)(elemType)
    case CEmptyObject => new InfiniteColumn with EmptyObjectColumn
    case CEmptyArray  => new InfiniteColumn with EmptyArrayColumn
    case CNull        => new InfiniteColumn with NullColumn
    case CUndefined => sys.error("Cannot construct constant column from CUndefined.")
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

  @inline def const[A: CValueType](v: IndexedSeq[A]) = new InfiniteColumn with HomogeneousArrayColumn[A] {
    val tpe = CArrayType(CValueType[A])
    def apply(row: Int) = v
  }

  def lift(col: Column): HomogeneousArrayColumn[_] = col match {
    case col: BoolColumn => new HomogeneousArrayColumn[Boolean] {
      val tpe = CArrayType(CBoolean)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Vector(col(row))
    }
    case col: LongColumn => new HomogeneousArrayColumn[Long] {
      val tpe = CArrayType(CLong)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Vector(col(row))
    }
    case col: NumColumn => new HomogeneousArrayColumn[BigDecimal] {
      val tpe = CArrayType(CNum)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Vector(col(row))
    }
    case col: StrColumn => new HomogeneousArrayColumn[String] {
      val tpe = CArrayType(CString)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Vector(col(row))
    }
    case col: DateColumn => new HomogeneousArrayColumn[DateTime] {
      val tpe = CArrayType(CDate)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int) = Vector(col(row))
    }
    case col: HomogeneousArrayColumn[a] => new HomogeneousArrayColumn[IndexedSeq[a]] {
      val tpe = CArrayType(col.tpe)
      def isDefinedAt(row: Int) = col.isDefinedAt(row)
      def apply(row: Int): IndexedSeq[IndexedSeq[a]] = Vector(col(row))
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
}
