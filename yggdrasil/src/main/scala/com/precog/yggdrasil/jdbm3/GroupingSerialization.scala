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

case class GroupingKey(columns: Array[CValue], ids: Identities)

object GroupingKeyComparator extends GroupingKeyComparator {
  final val serialVersionUID = 20120724l
}
  
class GroupingKeyComparator extends Comparator[GroupingKey] with Serializable {
  def readResolve() = GroupingKeyComparator

  def compare(a: GroupingKey, b: GroupingKey) = {
    // Compare over the key values first
    var result = 0
    var i = 0

    while (result == 0 && i < a.columns.length) {
      result = (a.columns(i),b.columns(i)) match {
        case (CString(as), CString(bs))   => as.compareTo(bs)
        case (CBoolean(ab), CBoolean(bb)) => ab.compareTo(bb)
        case (CLong(al), CLong(bl))       => al.compareTo(bl)
        case (CDouble(ad), CDouble(bd))   => ad.compareTo(bd)
        case (CNum(an), CNum(bn))         => an.bigDecimal.compareTo(bn.bigDecimal)
        case (CDate(ad), CDate(bd))       => ad.compareTo(bd)
        case (CNull, CNull)               => 0
        case (CEmptyObject, CEmptyObject) => 0
        case (CEmptyArray, CEmptyArray)   => 0
        case invalid                      => sys.error("Invalid comparison for GroupingKey of " + invalid)
      }
      i += 1
    }

    if (result == 0) {
      IdentitiesComparator.compare(a.ids, b.ids)
    } else {
      result
    }
  }
}
    
object GroupingKeySerializer extends GroupingKeySerializer {
  final val serialVersionUID = 20120724l
}

class GroupingKeySerializer extends Serializer[GroupingKey] with Serializable {
  def readResolve() = GroupingKeySerializer

  def serialize(out: DataOutput, gk: GroupingKey) {
    CValueSerializer.serialize(out, gk.columns)
    IdentitiesSerializer.serialize(out, gk.ids)
  }

  def deserialize(in: DataInput): GroupingKey = {
    GroupingKey(CValueSerializer.deserialize(in), IdentitiesSerializer.deserialize(in))
  }
}
 
