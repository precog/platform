package com.precog.yggdrasil
package table

import functions._

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
  @inline def const(v: Boolean) = new BoolColumn {
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = v
  }

  @inline def const(v: Long) = new LongColumn {
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = v
  }

  @inline def const(v: Double) = new DoubleColumn {
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = v
  }

  @inline def const(v: BigDecimal) = new NumColumn {
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = v
  }

  @inline def const(v: String) = new StrColumn {
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = v
  }

  @inline def const(v: DateTime) = new DateColumn {
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = v
  }

  object unionRightSemigroup extends Semigroup[Column] {
    def append(c1: Column, c2: => Column): Column = {
      UnionRight(c1, c2) getOrElse {
        sys.error("Illgal attempt to merge columns of dissimilar type: " + c1.tpe + "," + c2.tpe)
      }
    }
  }
}
