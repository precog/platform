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
package com.reportgrid.storage.leveldb

import org.iq80.leveldb._
import java.math.BigDecimal
import Bijection._

trait ColumnComparator[T] extends DBComparator {
  // Don't override unless you really know what you're doing
  def findShortestSeparator(start : Array[Byte], limit : Array[Byte]) = start
  def findShortSuccessor(key : Array[Byte]) = key
}

object ColumnComparator {
  implicit val longComparator = Some(new ColumnComparator[Long] {
    val name = "LongComparatorV1"
    def compare(a : Array[Byte], b : Array[Byte]) = {
      val valCompare = a.take(a.length - 8).as[Long].compareTo(b.take(b.length - 8).as[Long])

      if (valCompare == 0) {
        a.drop(a.length - 8).as[Long].compareTo(b.drop(b.length - 8).as[Long])
      } else {
        0
      }
    }
  })

  implicit val bigDecimalComparator = Some(new ColumnComparator[BigDecimal] {
    val name = "BigDecimalComparatorV1"
    def compare(a : Array[Byte], b : Array[Byte]) = {
      val valCompare = a.take(a.length - 8).as[BigDecimal].compareTo(b.take(b.length - 8).as[BigDecimal])

      if (valCompare == 0) {
        a.drop(a.length - 8).as[Long].compareTo(b.drop(b.length - 8).as[Long])
      } else {
        0
      }
    }
  })
}


// vim: set ts=4 sw=4 et:
