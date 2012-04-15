package com.precog.yggdrasil

trait Returning[@specialized(Boolean, Int, Long, Float, Double) A] { 
  def returns: CType { type CA = A }
}

trait F0[@specialized(Boolean, Int, Long, Float, Double) A] extends Returning[A] { outer =>
  def apply(row: Int): A

  def remap(f: Int => Int) = new F0[A] {
    val returns = outer.returns
    def apply(row: Int) = outer.apply(f(row))
  }

  def andThen[@specialized(Boolean, Int, Long, Float, Double) B](f: F1[_, B]): F0[B] = new F0[B] {
    val returns = f.returns
    def apply(row: Int): B = if (outer.returns == f.accepts) {
      outer.returns.cast1(f)(outer.returns.cast0(outer))(row)
    } else {
      sys.error("Argument type mismatch: required " + f.accepts + ", found " + outer.returns)
    }
  }
}

object F0 {
  def forArray[A](ctype: CType { type CA = A }, a: Array[A]): F0[A] = new F0[A] {
    val returns = ctype
    def apply(row: Int) = a(row)
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

trait CF1[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B] extends Returning[B] { outer =>
  def accepts: CType { type CA = A }
  def apply(a: A): B

  @inline final def applyCast(a: Any): B = apply(accepts.cast(a))

  final def compose[@specialized(Boolean, Int, Long, Float, Double) C](f: CF1[C, A]): CF1[C, B] = new CF1[C, B] {
    val accepts = f.accepts
    val returns = outer.returns
    def apply(c: C) = outer(f(c))
  }

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) C](f: CF1[B, C]): CF1[A, C] = new CF1[A, C] {
    val accepts = outer.accepts
    val returns = f.returns
    def apply(a: A) = f(outer(a))
  }

  final def toF1: F1[A, B] = new F1[A, B] {
    val accepts = outer.accepts
    val returns = outer.returns
    def apply(f0: F0[A]) = new F0[B] {
      val returns = outer.returns
      def apply(row: Int) = outer.apply(f0(row))
    }
  }
}

trait CF2[@specialized(Boolean, Int, Long, Float, Double) A, @specialized(Boolean, Int, Long, Float, Double) B, @specialized(Boolean, Int, Long, Float, Double) C] extends Returning[C] { outer =>
  def accepts: (CType { type CA = A }, CType { type CA = B })
  def apply(a: A, b: B): C

  @inline final def applyCast(a: Any, b: Any): C = apply(accepts._1.cast(a), accepts._2.cast(b))

  final def andThen[@specialized(Boolean, Int, Long, Float, Double) D](f: CF1[_, D]) = new CF2[A, B, D] {
    val accepts = outer.accepts
    val returns = f.returns
    def apply(a: A, b: B): D = f.applyCast(outer(a, b))
  }

  final def toF2: F2[A, B, C] = new F2[A, B, C] {
    val accepts = outer.accepts
    val returns = outer.returns
    def apply(fa: F0[A], fb: F0[B]) = new F0[C] {
      val returns = outer.returns
      def apply(row: Int) = outer.apply(fa(row), fb(row))
    }
  }
}

