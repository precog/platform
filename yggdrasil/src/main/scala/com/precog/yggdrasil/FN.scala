package com.precog.yggdrasil

trait Returning[@specialized(Boolean, Int, Long, Float, Double) A] { 
  def returns: CType { type CA = A }
}

trait F0[@specialized(Boolean, Int, Long, Float, Double) A] extends Returning[A] { outer =>
  def apply(row: Int): A

  def andThen[@specialized(Boolean, Int, Long, Float, Double) B](f: F1[_, B]): F0[B] = new F0[B] {
    val returns = f.returns
    def apply(row: Int): B = if (outer.returns == f.accepts) {
      outer.returns.cast1(f)(outer.returns.cast0(outer))(row)
    } else {
      sys.error("Argument type mismatch: required " + f.accepts + ", found " + outer.returns)
    }
  }
}

trait F1[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }
  def apply(a: F0[A]): F0[B]

  def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[C, A]): F1[C, B] = new F1[C, B] { 
    val accepts = f.accepts
    val returns = outer.returns
    def apply(c: F0[C]): F0[B] = (c andThen f) andThen outer
  }
  
  def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: F1[B, C]): F1[A, C] = new F1[A, C] { 
    val accepts = outer.accepts
    val returns = f.returns
    def apply(a: F0[A]): F0[C] = (a andThen outer) andThen f
  }
}

trait F2[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })
  def apply(a: F0[A], b: F0[B]): F0[C]

  def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: F1[_, D]) = new F2[A, B, D] {
    val accepts = outer.accepts
    val returns = f.returns
    def apply(a: F0[A], b: F0[B]): F0[D] = outer(a, b) andThen f
  }
}

// vim: set ts=4 sw=4 et:
