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
 
