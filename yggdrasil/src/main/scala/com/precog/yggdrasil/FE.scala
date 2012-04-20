package com.precog.yggdrasil

trait FE0[@specialized(Boolean, Int, Long, Float, Double) A] extends Returning[A] with (Int => A) { outer =>
  def apply(row: Int): A

  def remap(f: Int => Int): FE0[A] = new Remap(f) 

  def |> [@specialized(Boolean, Int, Long, Float, Double) B](f: FE1[A, B]): FE0[B] = new Thrush(f)

  private class Remap(f: Int => Int) extends FE0[A] {
    val returns = outer.returns
    def apply(row: Int) = outer(f(row))
  }

  private class Thrush[B](f: FE1[A, B]) extends FE0[B] {
    val returns = f.returns
    def apply(row: Int): B = f(outer)(row)
  }
}

object FE0 {
  def forArray[@specialized(Boolean, Int, Long, Float, Double) A](ctype: CType { type CA = A }, a: Array[A]): FE0[A] = new FE0[A] {
    val returns = ctype.asInstanceOf[CType { type CA = A }]
    def apply(row: Int) = a(row)
  }

  def const[@specialized(Boolean, Int, Long, Float, Double) A](ctype: CType { type CA = A }, a: A): FE0[A] = new FE0[A] {
    val returns = ctype
    def apply(row: Int) = a
  }
}

trait FE1[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }

  def apply(a: FE0[A]): FE0[B] 

  def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: FE1[C, A]): FE1[C, B] = new FE1[C, B] { 
    val accepts = f.accepts
    val returns = outer.returns
    def apply(c: FE0[C]): FE0[B] = c |> f |> outer
  }
  
  def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: FE1[B, C]): FE1[A, C] = new FE1[A, C] { 
    val accepts = outer.accepts
    val returns = f.returns
    def apply(a: FE0[A]): FE0[C] = a |> outer |> f
  }
}

trait FE2[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })

  def apply(a: FE0[A], b: FE0[B]): FE0[C]

  def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: FE1[C, D]) = new FE2[A, B, D] {
    val accepts = outer.accepts
    val returns = f.returns

    def apply(a: FE0[A], b: FE0[B]): FE0[D] = outer(a, b) |> f
  }
}

// Pure functions that can be promoted to FE1s.

trait FE1P[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }
  def apply(a: A): B

  final def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: FE1P[C, A]): FE1P[C, B] =  {
    new FE1P[C, B] {
      val accepts = f.accepts
      val returns = outer.returns
      def apply(c: C) = outer(f(c))
    }
  }

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: FE1P[B, C]): FE1P[A, C] = {
    new FE1P[A, C] {
      val accepts = outer.accepts
      val returns = f.returns
      def apply(a: A) = f(outer(a))
    }
  }

  final def toFE1: FE1[A, B] = {
    new FE1[A, B] {
      val accepts = outer.accepts
      val returns = outer.returns

      def apply(ca: FE0[A]): FE0[B] = 
        new FE0[B] {
          val returns = outer.returns
          def apply(row: Int) = outer.apply(ca(row))
        }
    }
  }
}

trait FE2P[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })
  def apply(a: A, b: B): C

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: FE1P[C, D]): FE2P[A, B, D] = {
    new FE2P[A, B, D] {
      val accepts = outer.accepts
      val returns = f.returns
      def apply(a: A, b: B): D = f(outer(a, b))
    }
  }

  final def toFE2: FE2[A, B, C] = {
    new FE2[A, B, C] {
      val accepts = outer.accepts
      val returns = outer.returns
      def apply(ca: FE0[A], cb: FE0[B]): FE0[C] = 
        new FE0[C] {
          val returns = outer.returns
          def apply(row: Int) = outer(ca(row), cb(row))
        }
    }
  }
}

// vim: set ts=4 sw=4 et:
