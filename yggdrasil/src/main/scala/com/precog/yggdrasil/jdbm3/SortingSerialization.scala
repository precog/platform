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

object SortingKeyComparator {
  final val serialVersionUID = 20120730l

  def apply(ascending: Boolean) = new SortingKeyComparator(ascending)
}
  
class SortingKeyComparator private[SortingKeyComparator] (val ascending: Boolean) extends Comparator[SortingKey] with Serializable {
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

    val finalResult = if (result == 0) {
      result = AscendingIdentitiesComparator.compare(a.ids, b.ids)
      if (result == 0) {
        a.index.compareTo(b.index)
      } else {
        result
      }
    } else {
      result
    }

    if (ascending) finalResult else -finalResult
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
 
