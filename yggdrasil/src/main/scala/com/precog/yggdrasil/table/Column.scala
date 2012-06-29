package com.precog.yggdrasil
package table

import cf._

import blueeyes.json.JsonAST._
import org.joda.time.DateTime

import scala.collection.mutable.BitSet
import scalaz.Semigroup
import scalaz.std.option._
import scalaz.syntax.apply._

sealed trait Column {
  def isDefinedAt(row: Int): Boolean
  def |> (f1: CF1): Option[Column] = f1(this)

  val tpe: CType
  def jValue(row: Int): JValue
  def strValue(row: Int): String

  def definedAt(from: Int, to: Int): BitSet = BitSet((for (i <- from until to if isDefinedAt(i)) yield i) : _*)
}

trait BoolColumn extends Column {
  def apply(row: Int): Boolean

  override val tpe = CBoolean
  override def jValue(row: Int) = JBool(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
}

trait LongColumn extends Column {
  def apply(row: Int): Long

  override val tpe = CLong
  override def jValue(row: Int) = JInt(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
}

trait DoubleColumn extends Column {
  def apply(row: Int): Double

  override val tpe = CDouble
  override def jValue(row: Int) = JDouble(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
}

trait NumColumn extends Column {
  def apply(row: Int): BigDecimal

  override val tpe = CDecimalArbitrary
  override def jValue(row: Int) = JDouble(this(row).toDouble)
  override def strValue(row: Int): String = this(row).toString
}

trait StrColumn extends Column {
  def apply(row: Int): String

  override val tpe = CStringArbitrary
  override def jValue(row: Int) = JString(this(row))
  override def strValue(row: Int): String = this(row)
}

trait DateColumn extends Column {
  def apply(row: Int): DateTime

  override val tpe = CDate
  override def jValue(row: Int) = JString(this(row).toString)
  override def strValue(row: Int): String = this(row).toString
}


trait EmptyArrayColumn extends Column {
  override val tpe = CEmptyArray
  override def jValue(row: Int) = JArray(Nil)
  override def strValue(row: Int): String = "[]"
}
object EmptyArrayColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyArrayColumn
}

trait EmptyObjectColumn extends Column {
  override val tpe = CEmptyObject
  override def jValue(row: Int) = JObject(Nil)
  override def strValue(row: Int): String = "{}"
}
object EmptyObjectColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyObjectColumn
}

trait NullColumn extends Column {
  override val tpe = CNull
  override def jValue(row: Int) = JNull
  override def strValue(row: Int): String = "null"
}
object NullColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with NullColumn
}


object Column {
  @inline def const(cv: CValue): Column = cv match {
    case CBoolean(v)  => const(v)
    case CLong(v)     => const(v)
    case CDouble(v)   => const(v)
    case CNum(v)      => const(v)
    case CString(v)   => const(v)
    case CDate(v)     => const(v)
    case CEmptyObject => new ConstColumn with EmptyObjectColumn 
    case CEmptyArray  => new ConstColumn with EmptyArrayColumn 
    case CNull        => new ConstColumn with NullColumn 
  }

  @inline def const(v: Boolean) = new ConstColumn with BoolColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: Long) = new ConstColumn with LongColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: Double) = new ConstColumn with DoubleColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: BigDecimal) = new ConstColumn with NumColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: String) = new ConstColumn with StrColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: DateTime) = new ConstColumn with DateColumn {
    def apply(row: Int) = v
  }

  object unionRightSemigroup extends Semigroup[Column] {
    def append(c1: Column, c2: => Column): Column = {
      cf.util.UnionRight(c1, c2) getOrElse {
        sys.error("Illgal attempt to merge columns of dissimilar type: " + c1.tpe + "," + c2.tpe)
      }
    }
  }
}
