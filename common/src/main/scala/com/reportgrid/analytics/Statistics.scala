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
package com.reportgrid.analytics

case class Statistics(n: Long, min: Double, max: Double, mean: Double, variance: Double, standardDeviation: Double)

object Statistics {
	def zero = Statistics(0, 0, 0, 0, 0, 0)
}

case class RunningStats(min: Double, max: Double, sum: Double, sumSq: Double, n: Long) {
  import IncrementalStatistics._

  def update(value: Double, n: Long): RunningStats = copy(
  	min   = this.min.min(value),
  	max   = this.max.max(value),
  	sum   = this.sum + (value * n),
  	sumSq = this.sumSq + n * (value * value),
  	n 	  = this.n + n
  )

  def statistics: Statistics = {
    if (n == 0) Statistics.zero 
    else Statistics(
      n = n, min = min, max = max, 
      mean = sum / n, 
      variance = variance(n, sum, sumSq), 
      standardDeviation = standardDeviation(n, sum, sumSq)
    )
  } 
}

object RunningStats {
  def zero = RunningStats(0, 0, 0, 0, 0L)
}

object IncrementalStatistics {
  def variance(n: Long, sum: Double, sumsq: Double) = (sumsq - ((sum * sum) / n)) / (n - 1)
  def standardDeviation(n: Long, sum: Double, sumsq: Double) = math.sqrt(variance(n, sum, sumsq))
}

