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
package com.precog.util

import scala.math.{max, min}

object Levenshtein {
  /**
   * Return the Levenshtein distance between s and t.
   *
   * There are three possible operations:
   *  1. delete a character
   *  2. insert a character
   *  3. substitute a character
   * 
   * The edit distance finds the smallest number of these operations
   * to get from s to t.
   */
  def distance(s: String, t: String): Int = {
    // handle some degenerate cases: strings are equal, or empty
    if (s == t) return 0
    val n = s.length
    val m = t.length
    if (n == 0) return m
    if (m == 0) return n

    // each vector represents a row of the Levenshtein algorithm.
    // for instance, if t="dog", the column headers would be:
    // ['', 'd', 'o', 'g']
    var arr0 = (0 to m).toArray
    var arr1 = new Array[Int](m + 1)

    var col = 0
    while (col < n) {
      arr1(0) = col + 1
      var row = 0
      while (row < m) {
        val cost = if (s.charAt(col) == t.charAt(row)) 0 else 1

        val north = arr1(row) + 1
        val west = arr0(row + 1) + 1
        val northwest = arr0(row) + cost
        arr1(row + 1) = min(min(north, west), northwest)

        row += 1
      }
      col += 1

      // swap our previous and current rows
      val tmp = arr0
      arr0 = arr1
      arr1 = tmp
    }

    // the last column of the final row has our result
    arr0(m)
  }

  /**
   * Return the normalized Levenshtein distance between s and t.
   *
   * This result is guaranteed to be in the interval [0, 1]. Since any
   * two strings have a maximum possible edit distance (replace as
   * much of the starting string as you want to use, and insert/delete
   * the rest) we can just normalize the absolute distance by this
   * maximum value, which is the maximum of s.length and t.length.
   */
  def normalized(s: String, t: String): Double = {
    distance(s, t).toDouble / max(s.length, t.length).toDouble
  }
}
