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
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.serialization.bijections._

import blueeyes.json.{JPath,JPathField,JPathIndex}

import scala.collection.JavaConverters._

import JDBMProjection._

/**
 * A slice built from a JDBMProjection with a backing array of key/value pairs
 *
 * @param source A source iterator of Map.Entry[Key,Value] pairs, positioned at the first element of the slice
 * @param size How many entries to retrieve in this slice
 */
trait JDBMSlice[Key,Value] extends Slice with Logging {
  protected def source: Iterator[java.util.Map.Entry[Key,Value]]
  protected def requestedSize: Int

  // This is storage for all data within this slice
  protected lazy val backing: Array[java.util.Map.Entry[Key,Value]] = source.take(requestedSize).toArray

  def size = backing.length

  def firstKey: Key = if (size > 0) backing(0).getKey else throw new NoSuchElementException("No keys in slice of size zero")
  def lastKey: Key  = if (size > 0) backing(size - 1).getKey else throw new NoSuchElementException("No keys in slice of size zero")
}

trait ArrayRowJDBMSlice[Key] extends JDBMSlice[Key,Array[CValue]] {
  trait BaseColumn {
    protected def columnIndex: Int
    protected def rowData(row: Int): Array[CValue]
    def isDefinedAt(row: Int) = row >= 0 && row < size && rowData(row).apply(columnIndex) != CUndefined
  }
  
  def columnFor(rowBacking: Int => Array[CValue], ref: ColumnRef, index: Int): (ColumnRef,Column) = ref -> (ref.ctype match {
    //// Fixed width types within the var width row
    case CBoolean => new BoolColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
      def apply(row: Int): Boolean = rowData(row).apply(index).asInstanceOf[CBoolean].value
    }

    case  CLong  => new LongColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
      def apply(row: Int): Long = rowData(row).apply(index).asInstanceOf[CLong].value
    }

    case CDouble => new DoubleColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
      def apply(row: Int): Double = rowData(row).apply(index).asInstanceOf[CDouble].value
    }

    case CDate => new DateColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
      def apply(row: Int): DateTime = new DateTime(rowData(row).apply(index).asInstanceOf[CLong].value)
    }

    case CNull => new NullColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
    }
    
    case CEmptyObject => new EmptyObjectColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
    }
    
    case CEmptyArray => new EmptyArrayColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
    }

    //// Variable width types
    case CString => new StrColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
      def apply(row: Int): String = rowData(row).apply(index).asInstanceOf[CString].value
    }

    case CNum => new NumColumn with BaseColumn {
      protected def columnIndex = index
      @inline def rowData(row: Int) = rowBacking(row)
      def apply(row: Int): BigDecimal = rowData(row).apply(index).asInstanceOf[CNum].value
    }

    case invalid => sys.error("Invalid fixed with CType: " + invalid)
  })
}

