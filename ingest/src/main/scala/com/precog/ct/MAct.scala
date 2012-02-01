package com.precog.ct

import scalaz._

/** Semigroup Action */
trait SAct[A, -B] { outer =>
  def append(a: A, b: => B): A
  def xappend(b: B, a: => A): A = append(a, b)
}

trait SActs {
  implicit def s2sact[A](implicit semigroup: Semigroup[A]): SAct[A, A] = new SAct[A, A] {
    override def append(a1: A, a2: => A): A = semigroup.append(a1, a2)
  }
}

object SAct extends SActs {
  def append[A, B](a: A, b: => B)(implicit sa: SAct[A, B]) = sa.append(a, b)

  implicit def a2w[A](a: A): SActW[A] = SActW(a)

  case class SActW[A](a: A) {
    def |+|[B](b: => B)(implicit sa: SAct[A, B]): A = sa.append(a, b)
  }
}

/** Monoid Action */
trait MAct[A, -B] extends SAct[A, B] {
  val zero: A
}

trait MActs {
  implicit def m2mact[A](implicit monoid: Monoid[A]): MAct[A, A] = new MAct[A, A] {
    override val zero: A = monoid.zero
    override def append(a1: A, a2: => A): A = monoid.append(a1, a2)
  }
}

object MAct extends MActs

trait Actions[M[_], A] {
  def value: M[A]

  def asuml[B](implicit fold: Foldable[M], mact: MAct[B, A]): B = {
    fold.foldLeft(value, mact.zero)(mact.append(_: B, _: A))
  }

  def asumr[B](implicit fold: Foldable[M], mact: MAct[B, A]): B = {
    fold.foldRight(value, mact.zero)(mact.xappend _)
  }
}

object Mult {
  type MDouble[A] = SAct[A, Double]
  
  object MDouble {
    implicit object MLongDouble extends MDouble[Long] {
      override def append(l: Long, d: => Double) = (l * d).round
    }
  }
}


// vim: set ts=4 sw=4 et:
