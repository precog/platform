package com.precog.common

import scala.math.Ordering

import java.math._

object JBigDecimalOrdering extends Ordering[BigDecimal] {
  def compare(a : BigDecimal, b : BigDecimal) = a.compareTo(b)
}
