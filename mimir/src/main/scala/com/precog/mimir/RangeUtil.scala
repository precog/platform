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

import com.precog.yggdrasil.table._

import scala.annotation.tailrec

object RangeUtil {
  /**
   * Loops through a Range much more efficiently than Range#foreach, running
   * the provided callback 'f' on each position. Assumes that step is 1.
   */
  def loop(r: Range)(f: Int => Unit) {
    var i = r.start
    val limit = r.end
    while (i < limit) {
      f(i)
      i += 1
    }
  }

  /**
   * Like loop but also includes a built-in check for whether the given Column
   * is defined for this particular row.
   */
  def loopDefined(r: Range, col: Column)(f: Int => Unit): Boolean = {
    @tailrec def unseen(i: Int, limit: Int): Boolean = if (i < limit) {
      if (col.isDefinedAt(i)) { f(i); seen(i + 1, limit) }
      else unseen(i + 1, limit)
    } else {
      false
    }

    @tailrec def seen(i: Int, limit: Int): Boolean = if (i < limit) {
      if (col.isDefinedAt(i)) f(i)
      seen(i + 1, limit)
    } else {
      true
    }

    unseen(r.start, r.end)
  }
}
