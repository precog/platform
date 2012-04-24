package com.precog.yggdrasil

trait Column[@specialized(Boolean, Int, Long, Float, Double) A] extends Returning[A] with (Int => A) { outer =>
  def isDefinedAt(row: Int): Boolean
  def apply(row: Int): A

  def remap(f: PartialFunction[Int, Int]): Column[A] = new Remap(f) with MemoizingColumn[A]

  def |> [@specialized(Boolean, Int, Long, Float, Double) B](f: F1[A, B]): Column[B] = new Thrush(f) with MemoizingColumn[B]

  private class Remap(f: PartialFunction[Int, Int]) extends Column[A] {
    val returns = outer.returns
    def isDefinedAt(row: Int): Boolean = f.isDefinedAt(row) && outer.isDefinedAt(f(row))
    def apply(row: Int) = outer(f(row))
  }

  private class Thrush[B](f: F1[A, B]) extends Column[B] {
    val returns = f.returns
    def isDefinedAt(row: Int) = f(outer).isDefinedAt(row)
    def apply(row: Int): B = f(outer)(row)
  }
}

trait MemoizingColumn[@specialized(Boolean, Int, Long, Float, Double) A] extends Column[A] {
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

object Column {
  def forArray[@specialized(Boolean, Int, Long, Float, Double) A](ctype: CType { type CA = A }, a: Array[A]): Column[A] = {
//    ctype match {
//      case v @ CBoolean          => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
//      case v @ CStringFixed(_)   => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
//      case v @ CStringArbitrary  => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
//      case v @ CInt              => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
//      case v @ CLong             => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
//      case v @ CFloat            => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
//      case v @ CDouble           => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
//      case v @ CDecimalArbitrary => v.cast(valueAt(m)) ?|? v.cast(other.valueAt(m))
//      case CNull | CEmptyArray | CEmptyObject => EQ
//    }

    new Column[A] {
      val returns = ctype.asInstanceOf[CType { type CA = A }]
      def isDefinedAt(row: Int) = row >= 0 && row < a.length
      def apply(row: Int) = a(row)
    }
  }

  def const[@specialized(Boolean, Int, Long, Float, Double) A](ctype: CType { type CA = A }, a: A): Column[A] = new Column[A] {
    val returns = ctype
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = a
  }
}

// vim: set ts=4 sw=4 et:
