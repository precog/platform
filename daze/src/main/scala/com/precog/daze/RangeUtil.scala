package com.precog
package daze

import com.precog.yggdrasil.table._

import scala.annotation.tailrec

object RangeUtil {
  /**
   * Loops through a Range much more efficiently than Range#foreach, running
   * the provided callback 'f' on each position. Assumes that step is 1.
   */
  def loop(r: Range)(f: Int => Unit) {
    var i = r.start
    val limit = r.end
    while (i < limit) {
      f(i)
      i += 1
    }
  }

  /**
   * Like loop but also includes a built-in check for whether the given Column
   * is defined for this particular row.
   */
  def loopDefined(r: Range, col: Column)(f: Int => Unit): Boolean = {
    @tailrec def unseen(i: Int, limit: Int): Boolean = if (i < limit) {
      if (col.isDefinedAt(i)) { f(i); seen(i + 1, limit) }
      else unseen(i + 1, limit)
    } else {
      false
    }

    @tailrec def seen(i: Int, limit: Int): Boolean = if (i < limit) {
      if (col.isDefinedAt(i)) f(i)
      seen(i + 1, limit)
    } else {
      true
    }

    unseen(r.start, r.end)
  }
}
