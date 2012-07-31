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

import java.io.{DataInput,DataOutput}
import java.util.Comparator

import scala.collection.BitSet

/**
 * A key for sorting tables to JDBM projections.
 *
 * @param columns The column values for the row. Undefind columns may be represented as any type, but CNull
 * is preferred
 * @param ids The identities for the row
 * @param index A synthetic index to allow differentiation of identical value/id combinations. These may be the
 * result of operations such as cross which result in cartesians
 */
case class SortingKey(columns: Array[CValue], ids: Identities, index: Long)

object SortingKeyComparator extends SortingKeyComparator {
  final val serialVersionUID = 20120730l
}
  
class SortingKeyComparator extends Comparator[SortingKey] with Serializable {
  def readResolve() = SortingKeyComparator

  def compare(a: SortingKey, b: SortingKey) = {
    // Compare over the key values first
    var result = 0
    var i = 0

    while (result == 0 && i < a.columns.length) {
      result = (a.columns(i),b.columns(i)) match {
        case (CUndefined, CUndefined)     => 0
        case (CUndefined, _)              => -1
        case (_, CUndefined)              => 1
        case (CString(as), CString(bs))   => as.compareTo(bs)
        case (CBoolean(ab), CBoolean(bb)) => ab.compareTo(bb)
        case (CLong(al), CLong(bl))       => al.compareTo(bl)
        case (CDouble(ad), CDouble(bd))   => ad.compareTo(bd)
        case (CNum(an), CNum(bn))         => an.bigDecimal.compareTo(bn.bigDecimal)
        case (CDate(ad), CDate(bd))       => ad.compareTo(bd)
        case (CNull, CNull)               => 0
        case (CEmptyObject, CEmptyObject) => 0
        case (CEmptyArray, CEmptyArray)   => 0
        case invalid                      => sys.error("Invalid comparison for SortingKey of " + invalid)
      }
      i += 1
    }

    if (result == 0) {
      result = IdentitiesComparator.compare(a.ids, b.ids)
      if (result == 0) {
        a.index.compareTo(b.index)
      } else {
        result
      }
    } else {
      result
    }
  }
}
    
object SortingKeySerializer {
  def apply(keyFormat: Array[CType], idCount: Int) = new SortingKeySerializer(keyFormat, idCount)
}

class SortingKeySerializer private[SortingKeySerializer](val keyFormat: Array[CType], val idCount: Int) extends Serializer[SortingKey] with Serializable {
  import CValueSerializerUtil.defaultSerializer

  final val serialVersionUID = 20120730l

  @transient
  private val keySerializer = CValueSerializer(keyFormat)
  @transient
  private val idSerializer  = IdentitiesSerializer(idCount)

  def serialize(out: DataOutput, gk: SortingKey) {
    defaultSerializer.serialize(out, new java.lang.Long(gk.index))
    keySerializer.serialize(out, gk.columns)
    idSerializer.serialize(out, gk.ids)
  }

  def deserialize(in: DataInput): SortingKey = {
    val index = defaultSerializer.deserialize(in).asInstanceOf[java.lang.Long]
    SortingKey(keySerializer.deserialize(in), idSerializer.deserialize(in), index)
  }
}
 
