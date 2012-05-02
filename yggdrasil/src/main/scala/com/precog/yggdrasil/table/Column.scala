package com.precog.yggdrasil
package table

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

case class ArrayColumn[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, data: Array[A], limit: Int) extends Column[A] {
  val returns = ctype.asInstanceOf[CType { type CA = A }]
  @inline final def isDefinedAt(row: Int) = row >= 0 && row < limit && row < data.length
  @inline final def apply(row: Int) = data(row)
  @inline final def update(row: Int, a: A) = data(row) = a
  @inline final def resize(limit: Int): ArrayColumn[A] = {
    assert(limit <= this.limit)
    ArrayColumn(ctype, data, limit)
  }

  override def toString = {
    "ArrayColumn(" + ctype + ", " + data.mkString("[", ",", "]") + ", " + limit + ")"
  }
}

object ArrayColumn {
  def apply[@specialized(Boolean, Long, Double) A: ClassManifest](ctype: CType { type CA = A }, size: Int): ArrayColumn[A] = ArrayColumn(ctype, new Array[A](size), size)
}

object Column {
  @inline def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A]): Column[A] = ArrayColumn[A](ctype, a, a.length)

  @inline def forArray[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: Array[A], limit: Int): Column[A] = ArrayColumn[A](ctype, a, limit)

  @inline def const[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, a: A): Column[A] = new Column[A] {
    val returns = ctype
    def isDefinedAt(row: Int) = true
    def apply(row: Int) = a
  }
}

// vim: set ts=4 sw=4 et:
