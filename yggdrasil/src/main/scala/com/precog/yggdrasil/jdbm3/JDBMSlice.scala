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

import com.precog.common.json._
import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import java.nio.ByteBuffer
import java.util.SortedMap

import com.precog.util.Bijection._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.serialization.bijections._

import scala.collection.JavaConverters._

import blueeyes.json.JPath

import scala.annotation.tailrec
import JDBMProjection._

object JDBMSlice {
  def load(size: Int, source: Iterator[java.util.Map.Entry[Array[Byte],Array[Byte]]], keyDecoder: ColumnDecoder, valDecoder: ColumnDecoder): (Array[Byte], Array[Byte], Int) = {
    var firstKey: Array[Byte] = null.asInstanceOf[Array[Byte]]
    var lastKey: Array[Byte]  = null.asInstanceOf[Array[Byte]]

    @tailrec
    def consumeRows(source: Iterator[java.util.Map.Entry[Array[Byte], Array[Byte]]], row: Int): Int = {
      if (source.hasNext) {
        val entry = source.next
        val rowKey = entry.getKey
        if (row == 0) { firstKey = rowKey }
        lastKey = rowKey

        keyDecoder.decodeToRow(row, rowKey)
        valDecoder.decodeToRow(row, entry.getValue)

        consumeRows(source, row + 1)
      } else {
        row
      }
    }
    
    val rows = consumeRows(source.take(size), 0)

    (firstKey, lastKey, rows)
  }

  def columnFor(prefix: CPath, sliceSize: Int)(ref: ColumnRef): (ColumnRef, ArrayColumn[_]) =
    (ref.copy(selector = (prefix \ ref.selector)), (ref.ctype match {
      case CString      => ArrayStrColumn.empty(sliceSize)
      case CBoolean     => ArrayBoolColumn.empty()
      case CLong        => ArrayLongColumn.empty(sliceSize)
      case CDouble      => ArrayDoubleColumn.empty(sliceSize)
      case CNum         => ArrayNumColumn.empty(sliceSize)
      case CDate        => ArrayDateColumn.empty(sliceSize)
      case CNull        => MutableNullColumn.empty()
      case CEmptyObject => MutableEmptyObjectColumn.empty()
      case CEmptyArray  => MutableEmptyArrayColumn.empty()
      case CUndefined   => sys.error("CUndefined cannot be serialized")
    }))
}

