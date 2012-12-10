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

import scala.annotation.tailrec
import scala.{ specialized => spec }

/**
 * This object contains some methods to do faster iteration over primitives.
 *
 * In particular it doesn't box, allocate intermediate objects, or use a (slow)
 * shared interface with scala collections.
 */
object Loop {
  @tailrec def range(i: Int, limit: Int)(f: Int => Unit) {
    if (i < limit) {
      f(i)
      range(i + 1, limit)(f)
    }
  }

  final def forall[@spec A](as: Array[A])(f: A => Boolean): Boolean = {
    @tailrec def loop(i: Int): Boolean = {
      if (i == as.length) true else if (f(as(i))) loop(i + 1) else false
    }

    loop(0)
  }
}
