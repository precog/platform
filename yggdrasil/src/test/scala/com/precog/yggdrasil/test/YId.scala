package com.precog.yggdrasil.test

import scalaz._


// An identity monad, since using scalaz's Id causes problem in for-comprehensions
// when A is structurally a Scala monad with flatMap et. al.
@deprecated("use scalaz.Need instead.", "2.7.0")
class YId[+A](a0: => A) {
  lazy val a = a0
}

@deprecated("use scalaz.Need instead.", "2.7.0")
trait YIdInstances {
  implicit lazy val M: Monad[YId] with Comonad[YId] = new Monad[YId] with Comonad[YId] with Cobind.FromCojoin[YId] {
    def point[A](a: => A) = new YId(a)
    def bind[A, B](m: YId[A])(f: A => YId[B]) = f(m.a)
    def cojoin[A](a: YId[A]): YId[YId[A]] = point(a)
    def copoint[A](y: YId[A]) = y.a
  }
}

@deprecated("use scalaz.Need instead.", "2.7.0")
object YId extends YIdInstances


// vim: set ts=4 sw=4 et:
