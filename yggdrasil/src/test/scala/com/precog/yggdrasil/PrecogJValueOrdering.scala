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
package com.precog.yggdrasil

import blueeyes.json._

import scalaz._
import scalaz.std.list._
import scalaz.std.tuple._

/**
 * This provides an ordering on JValue that mimics how we'd order them as
 * columns in a table, rather than using JValue's default ordering which
 * behaves differently.
 */
trait PrecogJValueOrder extends Order[JValue] {
  def order(a: JValue, b: JValue): Ordering = {
    val prims0 = a.flattenWithPath.toMap
    val prims1 = b.flattenWithPath.toMap
    val cols0 = (prims1.mapValues { _ => JUndefined } ++ prims0).toList.sorted
    val cols1 = (prims0.mapValues { _ => JUndefined } ++ prims1).toList.sorted
    Order[List[(JPath, JValue)]].order(cols0, cols1)
  }
}

object PrecogJValueOrder {
  implicit object order extends PrecogJValueOrder
  implicit def ordering: scala.math.Ordering[JValue] = order.toScalaOrdering
}
