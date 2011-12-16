package com.reportgrid.storage.leveldb
package comparators

import org.iq80.leveldb._
import java.math.BigDecimal
import Bijection._

trait ColumnComparator[T] extends DBComparator {
  // Don't override unless you really know what you're doing
  def findShortestSeparator(start : Array[Byte], limit : Array[Byte]) = start
  def findShortSuccessor(key : Array[Byte]) = key
}

class LongComparator extends ColumnComparator[Long] {
  val name = "LongComparatorV1"
  def compare(a : Array[Byte], b : Array[Byte]) = {
    val valCompare = a.take(a.length - 8).as[Long].compareTo(b.take(b.length - 8).as[Long])

    if (valCompare == 0) {
      a.drop(a.length - 8).as[Long].compareTo(b.drop(b.length - 8).as[Long])
    } else {
      0
    }
  }
}

class BigDecimalComparator extends ColumnComparator[BigDecimal] {
  val name = "BigDecimalComparatorV1"
  def compare(a : Array[Byte], b : Array[Byte]) = {
    val valCompare = a.take(a.length - 8).as[BigDecimal].compareTo(b.take(b.length - 8).as[BigDecimal])

    if (valCompare == 0) {
      a.drop(a.length - 8).as[Long].compareTo(b.drop(b.length - 8).as[Long])
    } else {
      0
    }
  }
}

object ColumnComparator {
  val Long = new LongComparator
  val BigDecimal = new BigDecimalComparator
}


// vim: set ts=4 sw=4 et:
