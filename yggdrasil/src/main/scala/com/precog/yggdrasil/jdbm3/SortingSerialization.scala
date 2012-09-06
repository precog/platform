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
package jdbm3

import org.apache.jdbm.Serializer

import java.io.{DataInput,DataOutput,Externalizable,ObjectInput,ObjectInputStream,ObjectOutput}
import java.util.Comparator

import scala.collection.BitSet

object SortingKeyComparator {
  final val serialVersionUID = 20120730l

  def apply(rowFormat: RowFormat, ascending: Boolean) = new SortingKeyComparator(rowFormat, ascending)
}

class SortingKeyComparator private[SortingKeyComparator] (rowFormat: RowFormat, ascending: Boolean)
    extends Comparator[Array[Byte]] with Serializable {

  def compare(a: Array[Byte], b: Array[Byte]) = {
    val selectors = rowFormat.columnRefs map (_.selector)
    val aVals = selectors zip rowFormat.decode(a) groupBy (_._1)
    val bVals = selectors zip rowFormat.decode(b) groupBy (_._1)

    val cmp = selectors.distinct.iterator map { cPath =>
      val a = aVals(cPath) find (_._2 != CUndefined)
      val b = bVals(cPath) find (_._2 != CUndefined)
      (a, b) match {
        case (None, None) => 0
        case (None, _) => -1
        case (_, None) => 1
        case (Some((_, a)), Some((_, b))) => CValue.compareValues(a, b)
      }
    } find (_ != 0) getOrElse 0

    if (ascending) cmp else -cmp
  }
}
