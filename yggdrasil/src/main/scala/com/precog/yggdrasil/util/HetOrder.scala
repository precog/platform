package com.precog.yggdrasil
package util

import com.precog.util.NumericComparisons

import scala.{ specialized => spec }

import spire.math.Order

import org.joda.time.DateTime

trait HetOrder[@spec(Boolean, Long, Double, AnyRef) A, @spec(Boolean, Long, Double, AnyRef) B] {
  def compare(a: A, b: B): Int
}

trait HetOrderLow {
  implicit def reverse[@spec(Boolean, Long, Double, AnyRef) A, @spec(Boolean, Long, Double, AnyRef) B](
      implicit ho: HetOrder[A, B]) = new HetOrder[B, A] {
    def compare(b: B, a: A) = {
      val cmp = ho.compare(a, b)
      if (cmp < 0) 1 else if (cmp == 0) 0 else -1
    }
  }

  implicit def fromOrder[@spec(Boolean, Long, Double, AnyRef) A](
      implicit o: Order[A]) = new HetOrder[A, A] {
    def compare(a: A, b: A) = o.compare(a, b)
  }
}

object HetOrder extends HetOrderLow {
  implicit object LongDoubleOrder extends HetOrder[Long, Double] {
    def compare(a: Long, b: Double): Int = NumericComparisons.compare(a, b)
  }

  implicit object LongBigDecimalOrder extends HetOrder[Long, BigDecimal] {
    def compare(a: Long, b: BigDecimal): Int = NumericComparisons.compare(a, b)
  }

  implicit object DoubleBigDecimalOrder extends HetOrder[Double, BigDecimal] {
    def compare(a: Double, b: BigDecimal): Int = NumericComparisons.compare(a, b)
  }
}

object ExtraOrders {
  implicit object BooleanOrder extends Order[Boolean] {
    def eqv(a: Boolean, b: Boolean) = a == b
    def compare(a: Boolean, b: Boolean) = if (a == b) 0 else if (a) 1 else -1
  }

  implicit object StringOrder extends Order[String] {
    def eqv(a: String, b: String) = a == b
    def compare(a: String, b: String) = a compareTo b
  }

  implicit object DateTimeOrder extends Order[DateTime] {
    def eqv(a: DateTime, b: DateTime) = compare(a, b) == 0
    def compare(a: DateTime, b: DateTime) = a compareTo b
  }
}
