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
package mimir

trait RegressionSupport {
  def Stats2Namespace = Vector("std", "stats")

  def dotProduct(xs: Array[Double], ys: Array[Double]): Double = {
    assert(xs.length == ys.length)
    var i = 0
    var result = 0.0
    while (i < xs.length) {
      result += xs(i) * ys(i)
      i += 1
    }
    result
  }    

  def arraySum(xs: Array[Double], ys: Array[Double]): Array[Double] = {
    assert(xs.length == ys.length)
    var i = 0
    var result = new Array[Double](xs.length)
    while (i < xs.length) {
      result(i) = xs(i) + ys(i)
      i += 1
    }
    result
  }    
}

