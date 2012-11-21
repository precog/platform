package com.precog.util

import scala.annotation.tailrec
import scala.{ specialized => spec }

/**
 * This object contains some methods to do faster iteration over primitives.
 *
 * In particular it doesn't box, allocate intermediate objects, or use a (slow)
 * shared interface with scala collections.
 */
object Loop {
  @tailrec def range(i: Int, limit: Int)(f: Int => Unit) {
    if (i < limit) {
      f(i)
      range(i + 1, limit)(f)
    }
  }

  final def forall[@spec A](as: Array[A])(f: A => Boolean): Boolean = {
    @tailrec def loop(i: Int): Boolean = {
      if (i == as.length) true else if (f(as(i))) loop(i + 1) else false
    }

    loop(0)
  }

  final def exists[@spec A](as: Array[A])(f: A => Boolean): Boolean = {
    @tailrec def loop(i: Int): Boolean = {
      if (i == as.length) false else if (f(as(i))) true else loop(i + 1)
    }

    loop(0)
  }
}
