package com.precog.yggdrasil
package table

import functions._

import scala.collection.mutable.BitSet
import scalaz.std.option._
import scalaz.syntax.apply._

trait Column[@specialized(Boolean, Long, Double) A] extends FN[A] { outer =>
  def isDefinedAt(row: Int): Boolean
  def apply(row: Int): A

  def remap(f: F1P[Int, Int]): Column[A] = new RemapColumn(f) with MemoizingColumn[A]

  def map[@specialized(Boolean, Long, Double) B](f: F1[A, B]): Column[B] = new MapColumn(f) with MemoizingColumn[B]

  private class RemapColumn(f: F1P[Int, Int]) extends Column[A] {
    val returns = outer.returns
    def isDefinedAt(row: Int): Boolean = f.isDefinedAt(row) && outer.isDefinedAt(f(row))
    def apply(row: Int) = outer(f(row))
  }

  private class MapColumn[B](f: F1[A, B]) extends Column[B] {
    val returns = f.returns
    def isDefinedAt(row: Int) = f(outer).isDefinedAt(row)
    def apply(row: Int): B = f(outer)(row)
  }

  def isCompatible(ctype: CType) = {
    CType.unify(returns, ctype).isDefined
  }

  def coerceInt: Option[Column[Int]] = {
    returns.asInstanceOf[CType] match {
      case CInt => Some(this.asInstanceOf[Column[Int]])
      case _ => None
    }
  }

  def coerceLong: Option[Column[Long]] = {
    returns.asInstanceOf[CType] match {
      case CInt => Some(this.asInstanceOf[Column[Int]].map(CoerceIntLong.toF1))
      case CLong => Some(this.asInstanceOf[Column[Long]])
      case _ => None
    }
  }

  def coerceFloat: Option[Column[Float]] = {
    returns.asInstanceOf[CType] match {
      case CInt => Some(this.asInstanceOf[Column[Int]].map(CoerceIntFloat.toF1))
      case CLong => Some(this.asInstanceOf[Column[Long]].map(CoerceLongFloat.toF1))
      case CFloat => Some(this.asInstanceOf[Column[Float]])
      case _ => None
    }
  }

  def coerceDouble: Option[Column[Double]] = {
    returns.asInstanceOf[CType] match {
      case CInt => Some(this.asInstanceOf[Column[Int]].map(CoerceIntDouble.toF1))
      case CLong => Some(this.asInstanceOf[Column[Long]].map(CoerceLongDouble.toF1))
      case CFloat => Some(this.asInstanceOf[Column[Float]].map(CoerceFloatDouble.toF1))
      case CDouble => Some(this.asInstanceOf[Column[Double]])
      case _ => None
    }
  }

  def coerceDecimal: Option[Column[BigDecimal]] = {
    returns.asInstanceOf[CType] match {
      case CInt => Some(this.asInstanceOf[Column[Int]].map(CoerceIntDecimal.toF1))
      case CLong => Some(this.asInstanceOf[Column[Long]].map(CoerceLongDecimal.toF1))
      case CFloat => Some(this.asInstanceOf[Column[Float]].map(CoerceFloatDecimal.toF1))
      case CDouble => Some(this.asInstanceOf[Column[Double]].map(CoerceDoubleDecimal.toF1))
      case CDecimalArbitrary => Some(this.asInstanceOf[Column[BigDecimal]])
      case _ => None
    }
  }
}

trait MemoizingColumn[@specialized(Boolean, Long, Double) A] extends Column[A] {
  private var _row = -1
  private var _value: A = _
  abstract override def apply(row: Int) = {
    if (_row != row) {
      _row = row
      _value = super.apply(row)
    }

    _value
  }
}

class ArrayColumn[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, data: Array[A], limit: Int, definedAt: BitSet) extends Column[A] {
  val returns = ctype//.asInstanceOf[CType { type CA = A }]
  @inline final def isDefinedAt(row: Int) = row >= 0 && row < limit && row < data.length && definedAt(row)
  @inline final def apply(row: Int) = data(row)

  @inline final def update(row: Int, a: A) = {
    data(row) = a
    definedAt(row) = true
  }

  @inline final def prefix(limit: Int): ArrayColumn[A] = {
    assert(limit <= this.limit && limit >= 0)
    new ArrayColumn(ctype, data, limit, definedAt)
  }

  override def toString = {
    val repr = (row: Int) => if (isDefinedAt(row)) apply(row).toString else '_'
    "ArrayColumn(" + ctype + ", " + (0 until limit).map(repr).mkString("[", ",", "]") + ", " + limit + ")"
  }
}

object ArrayColumn {
  def apply[@specialized(Boolean, Long, Double) A: ClassManifest](ctype: CType { type CA = A }, size: Int): ArrayColumn[A] = {
    val definedAt = new BitSet(size)
    for (i <- 0 until size) definedAt(i) = false
    new ArrayColumn(ctype, new Array[A](size), size, definedAt)
  }
}

sealed abstract class NullColumn(definedAt: BitSet) extends Column[Null] {
  @inline final def isDefinedAt(row: Int) = definedAt(row)
  @inline final def apply(row: Int) = null

  object defined {
    def update(row: Int, value: Boolean) = definedAt(row) = value
  }

  override def toString = {
    val limit = definedAt.reduce(_ max _)
    val repr = (row: Int) => if (definedAt(row)) 'x' else '_'
    "NullColumn(" + returns + ", " + (0 until limit).map(repr).mkString("[", ",", "]") + ", " + limit + ")"
  }
}

class CNullColumn(size: Int) extends NullColumn(new BitSet(size)) {
  val returns = CNull
}

class CEmptyObjectColumn(size: Int) extends NullColumn(new BitSet(size)) {
  val returns = CEmptyObject
}

class CEmptyArrayColumn(size: Int) extends NullColumn(new BitSet(size)) {
  val returns = CEmptyArray
}

object Column {
  def empty[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }): Column[A] = new Column[A] {
    val returns = ctype
    def isDefinedAt(row: Int) = false
    def apply(row: Int) = sys.error("Empty column dereferenced at row " + row)
  }

  @inline def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A]): Column[A] = forArray(ctype, a, a.length)

  @inline def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A], limit: Int): Column[A] = {
    val definedAt = new BitSet(limit)
    for (i <- 0 until limit) definedAt(i) = true
    new ArrayColumn[A](ctype, a, limit, definedAt)
  }

  @inline def const[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: A): Column[A] = new Column[A] {
    val returns = ctype
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = a
  }

  def unify(c1: Column[_], c2: Column[_]): Option[Column[_]] = {
    CType.unify(c1.returns, c2.returns) flatMap {
      case CInt => c1.coerceInt.flatMap(c1 => c2.coerceInt.map(c2 => F2.unify(CInt)(c1, c2)))
      case CLong => c1.coerceLong.flatMap(c1 => c2.coerceLong.map(c2 => F2.unify(CLong)(c1, c2)))
      case CFloat => c1.coerceFloat.flatMap(c1 => c2.coerceFloat.map(c2 => F2.unify(CFloat)(c1, c2)))
      case CDouble => c1.coerceDouble.flatMap(c1 => c2.coerceDouble.map(c2 => F2.unify(CDouble)(c1, c2)))
      case CDecimalArbitrary => c1.coerceDecimal.flatMap(c1 => c2.coerceDecimal.map(c2 => F2.unify(CDecimalArbitrary)(c1, c2)))
      case cstring => None
    }
  }
}

// vim: set ts=4 sw=4 et:
