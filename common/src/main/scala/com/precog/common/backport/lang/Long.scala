package com.precog.common.backport.lang

object Long {
  /** Backport of Java 7's java.lang.Long's static compare method */
  @inline
  def compare(x: Long, y: Long): Int = {
    if (x == y) 0
    else if (x < y) -1
    else 1
  }
}
