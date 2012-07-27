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
 * A slice built from a JDBMProjection
 *
 * @param source The source SortedMap containing the key/value pairs
 * @param descriptor The descriptor for the projection that this slice represents
 * @param size How many entries to retrieve in this slice
 */
class JDBMSlice private[jdbm3](source: IndexTree, descriptor: ProjectionDescriptor, requestedSize: Int) extends Slice with Logging {
  trait BaseColumn {
    def isDefinedAt(row: Int) = row < size
  }
  
  // This is where we store the full 2d array containing all values. Ugly cast to allow us to use IndexedSeq internally
  private[this] val backing: Array[java.util.Map.Entry[Identities,IndexedSeq[CValue]]] = source.entrySet().iterator().asScala.take(size).toArray.asInstanceOf[Array[java.util.Map.Entry[Identities,IndexedSeq[CValue]]]]

  def size = backing.length

  case class IdentColumn(index: Int) extends LongColumn with BaseColumn {
    def apply(row: Int): Long = backing(row).getKey.apply(index)
  }

  protected def keyColumns: Map[ColumnRef, Column] = (0 until descriptor.identities).map {
    idx: Int => ColumnRef(JPath(JPathField("key") :: JPathIndex(idx) :: Nil), CLong) -> IdentColumn(idx)
  }.toMap

  def valColumns: Seq[(ColumnRef, Column)] = descriptor.columns.zipWithIndex.map {
    case (ColumnDescriptor(_, selector, ctpe, _),index) => ColumnRef(selector, ctpe) -> (ctpe match {
      //// Fixed width types within the var width row
      case CBoolean => new BoolColumn with BaseColumn {
        def apply(row: Int): Boolean = backing(row).getValue().apply(index).asInstanceOf[java.lang.Boolean]
      }

      case  CLong  => new LongColumn with BaseColumn {
        def apply(row: Int): Long = backing(row).getValue().apply(index).asInstanceOf[java.lang.Long]
      }

      case CDouble => new DoubleColumn with BaseColumn {
        def apply(row: Int): Double = backing(row).getValue().apply(index).asInstanceOf[java.lang.Double]
      }

      case CDate => new DateColumn with BaseColumn {
        def apply(row: Int): DateTime = new DateTime(backing(row).getValue().apply(index).asInstanceOf[java.lang.Long])
      }

      case CNull => LNullColumn
      
      case CEmptyObject => LEmptyObjectColumn
      
      case CEmptyArray => LEmptyArrayColumn

      //// Variable width types
      case CString => new StrColumn with BaseColumn {
        def apply(row: Int): String = backing(row).getValue().apply(index).asInstanceOf[String]
      }

      case CNum => new NumColumn with BaseColumn {
        def apply(row: Int): BigDecimal = BigDecimal(backing(row).getValue().apply(index).asInstanceOf[java.math.BigDecimal])
      }

      case invalid => sys.error("Invalid fixed with CType: " + invalid)
    })
  }

  lazy val columns: Map[ColumnRef, Column] = keyColumns ++ valColumns
  
  object LNullColumn extends table.NullColumn with BaseColumn
  object LEmptyObjectColumn extends table.EmptyObjectColumn with BaseColumn
  object LEmptyArrayColumn extends table.EmptyArrayColumn with BaseColumn
}
