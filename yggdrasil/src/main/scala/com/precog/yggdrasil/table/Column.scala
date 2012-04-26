package com.precog.yggdrasil
package table

trait Column[@specialized(Boolean, Long, Double) A] extends FN[A] with (Int => A) { outer =>
  def isDefinedAt(row: Int): Boolean
  def apply(row: Int): A

  def remap(f: PartialFunction[Int, Int]): Column[A] = new RemapColumn(f) with MemoizingColumn[A]

  def map[@specialized(Boolean, Long, Double) B](f: F1[A, B]): Column[B] = new MapColumn(f) with MemoizingColumn[B]

  private class RemapColumn(f: PartialFunction[Int, Int]) extends Column[A] {
    val returns = outer.returns
    def isDefinedAt(row: Int): Boolean = f.isDefinedAt(row) && outer.isDefinedAt(f(row))
    def apply(row: Int) = outer(f(row))
  }

  private class MapColumn[B](f: F1[A, B]) extends Column[B] {
    val returns = f.returns
    def isDefinedAt(row: Int) = f(outer).isDefinedAt(row)
    def apply(row: Int): B = f(outer)(row)
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

object Column {
  def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A]): Column[A] = {
    new Column[A] {
      val returns = ctype.asInstanceOf[CType { type CA = A }]
      def isDefinedAt(row: Int) = row >= 0 && row < a.length
      def apply(row: Int) = a(row)
    }
  }

  def const[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: A): Column[A] = new Column[A] {
    val returns = ctype
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = a
  }
}

// vim: set ts=4 sw=4 et:
