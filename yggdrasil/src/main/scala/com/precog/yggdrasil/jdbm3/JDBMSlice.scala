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

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import java.nio.ByteBuffer
import java.util.SortedMap

import com.precog.util.Bijection._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.serialization.bijections._

import scala.collection.JavaConverters._

import blueeyes.json.JPath

import JDBMProjection._

/**
 * A slice built from a JDBMProjection with a backing array of key/value pairs
 *
 * @param source A source iterator of Map.Entry[Key,Array[Byte]] pairs, positioned at the first element of the slice
 * @param size How many entries to retrieve in this slice
 */
trait JDBMSlice[Key] extends Slice with Logging {
  protected def requestedSize: Int

  protected def keyColumns: Array[(ColumnRef, ArrayColumn[_])]
  protected def valColumns: Array[(ColumnRef, ArrayColumn[_])]

  def columnDecoder: ColumnDecoder

  // This method is responsible for loading the data from the key at the given row,
  // most likely into one or more of the key columns defined above
  protected def loadRowFromKey(row: Int, key: Key): Unit

  private var row = 0

  protected def load(source: Iterator[java.util.Map.Entry[Key,Array[Byte]]]) {
    source.take(requestedSize).foreach {
      entry => {
        loadRowFromKey(row, entry.getKey)
        columnDecoder.decodeToRow(row, entry.getValue)
        row += 1
      }
    }
  }

  def size = row

  def columns: Map[ColumnRef, Column] = keyColumns.++(valColumns)(collection.breakOut)
}

object JDBMSlice {
  def columnFor(prefix: JPath, sliceSize: Int)(ref: ColumnRef): (ColumnRef, ArrayColumn[_]) =
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

