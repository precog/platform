/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog
package ragnarok

import blueeyes.json._

import scalaz._
import scalaz.std.option._


/**
 * Holds statistics for some metric. These statistics are only valid if
 * `n > 2 * tails`. If `n <= 2 * tails`, then the mean and variance will be
 * `NaN` and the count will be `0`.
 */
case class Statistics private[ragnarok] (
    tails: Int,
    allMin: List[Double],
    allMax: List[Double],
    m: Double,
    vn: Double,
    n: Int) {

  //FIXME: keep track of Double error

  /**
   * Multiply this statistic by some constant > 0. Using this is equivalent to
   * computing the stats of a scaled (by `x`) version of the original dataset.
   * This can be safely used to convert between units, after the fact, for
   * example.
   */
  def *(x: Double): Statistics = if (x >= 0.0) {
    Statistics(tails, allMin map (_ * x), allMax map (_ * x), m * x, vn * x * x, n)
  } else {
    Statistics(tails, allMax map (_ * x), allMin map (_ * x), m * x, vn * math.abs(x) * math.abs(x), n)
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

  def toJson: JObject = JObject(List(
    JField("mean", JNum(mean)),
    JField("variance", JNum(variance)),
    JField("stdDev", JNum(stdDev)),
    JField("min", JNum(min)),
    JField("max", JNum(max)),
    JField("count", JNum(count))))
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


