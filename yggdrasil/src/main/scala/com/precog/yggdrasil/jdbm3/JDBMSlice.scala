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
  protected val backing: Array[java.util.Map.Entry[Key,Value]] = source.take(size).toArray

  def size = backing.length

  def firstKey: Key = backing(0).getKey
  def lastKey: Key  = backing(size - 1).getKey
}

trait ArrayRowJDBMSlice[Key] extends JDBMSlice[Key,Array[CValue]] {
  trait BaseColumn {
    def isDefinedAt(row: Int) = row < size
  }
  
  def columnFor(rowData: Int => Array[CValue], ref: ColumnRef, index: Int): (ColumnRef,Column) = ref -> (ref.ctype match {
    //// Fixed width types within the var width row
    case CBoolean => new BoolColumn with BaseColumn {
      def apply(row: Int): Boolean = rowData(row).apply(index).asInstanceOf[java.lang.Boolean]
    }

    case  CLong  => new LongColumn with BaseColumn {
      def apply(row: Int): Long = rowData(row).apply(index).asInstanceOf[java.lang.Long]
    }

    case CDouble => new DoubleColumn with BaseColumn {
      def apply(row: Int): Double = rowData(row).apply(index).asInstanceOf[java.lang.Double]
    }

    case CDate => new DateColumn with BaseColumn {
      def apply(row: Int): DateTime = new DateTime(rowData(row).apply(index).asInstanceOf[java.lang.Long])
    }

    case CNull => LNullColumn
    
    case CEmptyObject => LEmptyObjectColumn
    
    case CEmptyArray => LEmptyArrayColumn

    //// Variable width types
    case CString => new StrColumn with BaseColumn {
      def apply(row: Int): String = rowData(row).apply(index).asInstanceOf[String]
    }

    case CNum => new NumColumn with BaseColumn {
      def apply(row: Int): BigDecimal = BigDecimal(rowData(row).apply(index).asInstanceOf[java.math.BigDecimal])
    }

    case invalid => sys.error("Invalid fixed with CType: " + invalid)
  })

  object LNullColumn extends table.NullColumn with BaseColumn
  object LEmptyObjectColumn extends table.EmptyObjectColumn with BaseColumn
  object LEmptyArrayColumn extends table.EmptyArrayColumn with BaseColumn
}

