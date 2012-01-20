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
package com.reportgrid.yggdrasil
package leveldb

import com.reportgrid.util.Bijection._
import java.nio.ByteBuffer

private[leveldb] class LevelDBBuffer(length: Int) {
  private final val buf = ByteBuffer.allocate(length)
  def writeIdentity(id: Identity): Unit = buf.putLong(id)
  def writeValue(col: ColumnDescriptor, v: CValue) = v.fold[Unit](
    str     = s => {
      val sbytes = s.getBytes("UTF-8")
      col.qsel.valueType.format match {
        case FixedWidth(w) => buf.put(sbytes, 0, w)
        case LengthEncoded => buf.putInt(sbytes.length).put(sbytes)
      }
    },
    bool    = b => buf.put(if (b) 0x01 else 0x00), 
    int     = i => buf.putInt(i), 
    long    = l => buf.putLong(l),
    float   = f => buf.putFloat(f), 
    double  = d => buf.putDouble(d), 
    num     = d => {
      val dbytes = d.as[Array[Byte]]
      buf.putInt(dbytes.length).put(dbytes)
    },
    emptyObj = (), emptyArr = (), 
    nul = col.qsel.valueType match {
      case SStringFixed(width)    => buf.put(Array.fill[Byte](width)(0x00))
      case SStringArbitrary       => buf.putInt(0)
      case SBoolean               => buf.put(0xFF)
      case SInt                   => buf.putInt(Int.MaxValue)
      case SLong                  => buf.putLong(Long.MaxValue)
      case SFloat                 => buf.putFloat(Float.MaxValue)
      case SDouble                => buf.putDouble(Double.MaxValue)
      case SDecimalArbitrary      => buf.putInt(0)
      case _                      => ()
    }
  )

  def toArray: Array[Byte] = buf.array
}

trait LevelDBByteProjection extends ByteProjection {
  private val incompatible = (_: Any) => sys.error("Column values incompatible with projection descriptor.")

  def project(identities: Identities, cvalues: Seq[CValue]): (Array[Byte], Array[Byte]) = {
    lazy val valueWidths = descriptor.columns.map(_.qsel.valueType.format) zip cvalues map {
      case (FixedWidth(w), sv) => w
      case (LengthEncoded, sv) => 
        sv.fold[Int](
          str     = s => s.getBytes("UTF-8").length + 4,
          bool    = incompatible, int     = incompatible, long    = incompatible,
          float   = incompatible, double  = incompatible, 
          num     = _.as[Array[Byte]].length + 4,
          emptyObj = incompatible(()), emptyArr = incompatible(()), nul = incompatible(())
        )
    }

    val (usedIdentities, usedValues, indexWidth) = descriptor.sorting.foldLeft((Set.empty[Int], Set.empty[Int], 0)) { 
      case ((ids, values, width), (col, ById)) => 
        (ids + descriptor.indexedColumns(col), values, width + 8)

      case ((ids, values, width), (col, ByValue)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (ids, values + valueIndex, width + valueWidths(valueIndex))

      case ((ids, values, width), (col, ByValueThenId)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (ids + descriptor.indexedColumns(col), values + valueIndex, width + valueWidths(valueIndex) + 8)
    }

    // all of the identities must be included in the key; also, any values of columns that
    // use by-value ordering must be included in tthe key. 
    val indexBuffer = new LevelDBBuffer(indexWidth + ((identities.size - usedIdentities.size) * 8))
    descriptor.sorting.foreach {
      case (col, ById)          => indexBuffer.writeIdentity(identities(descriptor.indexedColumns(col)))
      case (col, ByValue)       => indexBuffer.writeValue(col, cvalues(descriptor.indexedColumns(col)))
      case (col, ByValueThenId) =>
        indexBuffer.writeValue(col, cvalues(descriptor.indexedColumns(col)))
        indexBuffer.writeIdentity(identities(descriptor.indexedColumns(col)))
    }

    identities.zipWithIndex.foreach {
      case (id, i) => if (!usedIdentities.contains(i)) indexBuffer.writeIdentity(id)
    }

    val valuesBuffer = new LevelDBBuffer(valueWidths.zipWithIndex collect { case (w, i) if !usedValues.contains(i) => w } sum)
    (cvalues zip descriptor.columns).zipWithIndex.foreach {
      case ((v, col), i) if !usedValues.contains(i) => valuesBuffer.writeValue(col, v)
    }

    (indexBuffer.toArray, valuesBuffer.toArray)
  }

  def unproject[E](keyBytes: Array[Byte], valueBytes: Array[Byte])(f: (Identities, Seq[CValue]) => E): E = {
    sys.error("todo")
  }
}

// vim: set ts=4 sw=4 et:
