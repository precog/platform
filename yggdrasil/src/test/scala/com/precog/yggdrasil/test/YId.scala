package com.precog.yggdrasil.test

import scalaz._


// An identity monad, since using scalaz's Id causes problem in for-comprehensions
// when A is structurally a Scala monad with flatMap et. al.
class YId[+A](a0: => A) {
  lazy val a = a0
}

trait YIdInstances {
  implicit val M: Monad[YId] with Copointed[YId] = new Monad[YId] with Copointed[YId] {
    def point[A](a: => A) = new YId(a)
    def bind[A, B](m: YId[A])(f: A => YId[B]) = f(m.a)
    def copoint[A](y: YId[A]) = y.a
  }
}

object YId extends YIdInstances


// vim: set ts=4 sw=4 et:
