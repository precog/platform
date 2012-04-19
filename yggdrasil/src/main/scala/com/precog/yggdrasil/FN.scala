package com.precog.yggdrasil

trait Returning[@specialized(Boolean, Int, Long, Float, Double) A] { 
  def returns: CType { type CA = A }
}

trait F1[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }

  def isDefinedAt(a: Column[A]): Int => Boolean

  def apply(a: Column[A]): Column[B] 

  def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[C, A]): F1[C, B] = new F1[C, B] { 
    val accepts = f.accepts
    val returns = outer.returns
    def isDefinedAt(c: Column[C]) = f andThen outer isDefinedAt c
    def apply(c: Column[C]): Column[B] = c |> f |> outer
  }
  
  def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[B, C]): F1[A, C] = new F1[A, C] { 
    val accepts = outer.accepts
    val returns = f.returns
    def isDefinedAt(a: Column[A]) = outer andThen f isDefinedAt a
    def apply(a: Column[A]): Column[C] = a |> outer |> f
  }
}

trait F2[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })

  def isDefinedAt(a: Column[A], b: Column[B]): Int => Boolean

  def apply(a: Column[A], b: Column[B]): Column[C]

  def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: F1[C, D]) = new F2[A, B, D] {
    val accepts = outer.accepts
    val returns = f.returns

    def isDefinedAt(a: Column[A], b: Column[B]): Int => Boolean = f.isDefinedAt(outer(a, b))
    def apply(a: Column[A], b: Column[B]): Column[D] = (new FooCol(a, b)) |> f
  }

  //TODO: Minimize the scalac bug...
  private class FooCol(a: Column[A], b: Column[B]) extends Col(a, b) with MemoizingColumn[C]

  private class Col(a: Column[A], b: Column[B]) extends Column[C] {
    val returns = outer.returns
    def isDefinedAt(row: Int): Boolean = outer.isDefinedAt(a, b)(row)
    def apply(row: Int): C = outer(a, b)(row)
  }
}

// Pure functions that can be promoted to F1s.

trait F1P[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }
  def isDefinedAt(a: A): Boolean
  def apply(a: A): B

  @inline final def applyCast(a: Any): B = apply(accepts.cast(a))

  final def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: F1P[C, A]): F1P[C, B] =  {
    new F1P[C, B] {
      private var _arg: C = _
      private var _value: B = _

      val accepts = f.accepts
      val returns = outer.returns
      def isDefinedAt(c: C): Boolean = outer.isDefinedAt(f(c))
      def apply(c: C) = {
        if (_arg != c) {
          _arg = c
          _value = outer(f(c))
        }

        _value
      }
    }
  }

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: F1P[B, C]): F1P[A, C] = {
    new F1P[A, C] {
      private var _arg: A = _
      private var _value: C = _

      val accepts = outer.accepts
      val returns = f.returns
      def isDefinedAt(a: A) = f.isDefinedAt(outer(a))
      def apply(a: A) = {
        if (_arg != a) {
          _arg = a
          _value = f(outer(a))
        }

        _value
      }
    }
  }

  final def toF1: F1[A, B] = {
    new F1[A, B] {
      private var _ca: Column[A] = _
      private var _value: Column[B] = _

      val accepts = outer.accepts
      val returns = outer.returns

      def isDefinedAt(ca: Column[A]): Int => Boolean = 
        apply(ca).isDefinedAt _

      def apply(ca: Column[A]): Column[B] = {
        if (ca != _ca) {
          _ca = ca
          _value = new FooCol(_ca)
        }

        _value
      }
    }
  }

  private class FooCol(ca: Column[A]) extends Col(ca) with MemoizingColumn[B]
  private class Col(ca: Column[A]) extends Column[B] {
    val returns = outer.returns
    def isDefinedAt(row: Int) = ca.isDefinedAt(row) && outer.isDefinedAt(ca(row))
    def apply(row: Int) = outer.apply(ca(row))
  }
}

trait F2P[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })
  def isDefinedAt(a: A, b: B): Boolean
  def apply(a: A, b: B): C

  @inline final def applyCast(a: Any, b: Any): C = apply(accepts._1.cast(a), accepts._2.cast(b))

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: F1P[C, D]): F2P[A, B, D] = {
    new F2P[A, B, D] {
      val accepts = outer.accepts
      val returns = f.returns
      def isDefinedAt(a: A, b: B) = f.isDefinedAt(outer(a, b))
      def apply(a: A, b: B): D = f(outer(a, b))
    }
  }

  final def toF2: F2[A, B, C] = {
    new F2[A, B, C] {
      private var _ca: Column[A] = _
      private var _cb: Column[B] = _
      private var _value: Column[C] = _

      val accepts = outer.accepts
      val returns = outer.returns

      def isDefinedAt(ca: Column[A], cb: Column[B]): Int => Boolean = 
        apply(ca, cb).isDefinedAt _

      def apply(ca: Column[A], cb: Column[B]): Column[C] = {
        if ((ca != _ca) || (cb != _cb)) {
          _ca = ca
          _cb = cb
          _value = new FooCol(ca, cb) 
        }

        _value
      }
    }
  }

  private class FooCol(ca: Column[A], cb: Column[B]) extends Col(ca, cb) with MemoizingColumn[C]
  private class Col(ca: Column[A], cb: Column[B]) extends Column[C] {
    val returns = outer.returns
    def isDefinedAt(row: Int) = ca.isDefinedAt(row) && cb.isDefinedAt(row) && outer.isDefinedAt(ca(row), cb(row))
    def apply(row: Int) = outer.apply(ca(row), cb(row))
  }
}

