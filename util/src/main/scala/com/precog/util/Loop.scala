package com.precog.util

import scala.annotation.tailrec

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
}
