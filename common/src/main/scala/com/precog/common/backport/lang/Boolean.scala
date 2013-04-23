package com.precog.common.backport.lang

object Boolean {
  /** Backport of Java 7's java.lang.Boolean's static compare method */
  @inline
  def compare(x: Boolean, y: Boolean): Int = {
    if (x == y) 0
    else if (!x && y) -1
    else 1
  }
}
