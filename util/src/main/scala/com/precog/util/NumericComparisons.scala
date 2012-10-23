package com.precog.util

object NumericComparisons {

  @inline def compare(a: Long, b: Long): Int = if (a < b) -1 else if (a == b) 0 else 1

  @inline def compare(a: Long, b: Double): Int = -compare(b, a)

  @inline def compare(a: Long, b: BigDecimal): Int = BigDecimal(a) compare b

  def compare(a: Double, bl: Long): Int = {
    val b = bl.toDouble
    if (b.toLong == bl) {
      if (a < b) -1 else if (a == b) 0 else 1
    } else {
      val error = math.abs(b * 2.220446049250313E-16)
      if (a < b - error) -1 else if (a > b + error) 1 else bl.signum
    }
  }

  @inline def compare(a: Double, b: Double): Int = if (a < b) -1 else if (a == b) 0 else 1

  @inline def compare(a: Double, b: BigDecimal): Int = BigDecimal(a) compare b

  @inline def compare(a: BigDecimal, b:Long): Int = a compare BigDecimal(b)

  @inline def compare(a: BigDecimal, b:Double): Int = a compare BigDecimal(b)

  @inline def compare(a: BigDecimal, b:BigDecimal): Int = a compare b

  @inline def eps(b: Double): Double = math.abs(b * 2.220446049250313E-16)

  def approxCompare(a: Double, b: Double): Int = {
    val aError = eps(a)
    val bError = eps(b)
    if (a + aError < b - bError) -1 else if (a - aError > b + bError) 1 else 0
  }


  import scalaz.Ordering.{ LT, GT, EQ }

  @inline def order(a: Long, b: Long): scalaz.Ordering =
    if (a < b) LT else if (a == b) EQ else GT

  @inline def order(a: Double, b: Double): scalaz.Ordering =
    if (a < b) LT else if (a == b) EQ else GT

  @inline def order(a: Long, b: Double): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Double, b: Long): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Long, b: BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: Double, b: BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b:Long): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b:Double): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))

  @inline def order(a: BigDecimal, b:BigDecimal): scalaz.Ordering =
    scalaz.Ordering.fromInt(compare(a, b))
}
