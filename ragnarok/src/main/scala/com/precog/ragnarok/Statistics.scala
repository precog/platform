package com.precog.ragnarok

import scalaz._
import scalaz.std.option._


/**
 * Holds statistics for some metric. These statistics are only valid if
 * `n > 2 * tails`. If `n <= 2 * tails`, then the mean and variance will be
 * `NaN` and the count will be `0`.
 */
case class Statistics private (
    tails: Int,
    allMin: List[Double],
    allMax: List[Double],
    m: Double,
    vn: Double,
    n: Int) {

  /**
   * Multiply this statistic by some constant > 0.
   */
  def *(x: Double): Statistics = if (x >= 0.0) {
    Statistics(tails, allMin map (_ * x), allMax map (_ * x), m * x, vn * x, n)
  } else {
    Statistics(tails, allMax map (_ * x), allMin map (_ * x), m * x, vn * math.abs(x), n)
  }


  def +(x: Double): Statistics = this + Statistics(x, tails = tails)

  def +(that: Statistics): Statistics = Statistics.semigroup.append(this, that)


  // Calculates the mean, variance, and count without outliers.
  private lazy val meanVarCount: (Double, Double, Int) = if (n > 2 * tails) {
    (allMin.reverse.tail ++ allMax.tail).foldLeft((m, vn, n)) {
      case ((m, vn, n), x) =>
        val mprev = m + (m - x) / (n - 1)
        val sprev = vn - (x - mprev) * (x - m)
        (mprev, sprev, n - 1)
    } match {
      case (m, vn, 1) => (m, 0.0, 1)
      case (m, vn, n) => (m, math.abs(vn / (n - 1)), n)
    }
  } else {
    (Double.NaN, Double.NaN, 0)
  }

  def min: Double = allMin.last
  
  def max: Double = allMax.head

  def mean: Double = meanVarCount._1

  def variance: Double = meanVarCount._2

  def stdDev: Double = math.sqrt(variance)

  def count: Int = meanVarCount._3
}


object Statistics {
  def apply(x: Double, tails: Int = 0): Statistics =
    Statistics(tails, x :: Nil, x :: Nil, x, 0.0, 1)

  /**
   * The `Statistics` semigroup mixes 2 `Statistics` together. The total number
   * of outliers kept is the minimum of the 2 `Statistics`, as this is the only
   * way to ensure associativity.
   */
  implicit object semigroup extends Semigroup[Statistics] { 
    def append(x: Statistics, _y: => Statistics): Statistics = {
      val y = _y

      val z_m = x.m + (y.n / (x.n + y.n).toDouble) * (y.m - x.m)
      val z_vn = x.vn + y.vn + y.n * (y.m - x.m) * (y.m - z_m)
      val z_tails = x.tails min y.tails

      Statistics(z_tails,
        (x.allMin ++ y.allMin).sorted take (z_tails + 1),
        (x.allMax ++ y.allMax).sorted takeRight (z_tails + 1),
        z_m, z_vn, x.n + y.n)
    }
  }
}


