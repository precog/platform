package com.precog.yggdrasil
package jdbm3

import org.apache.jdbm.Serializer

import java.io.{DataInput,DataOutput,Externalizable,ObjectInput,ObjectInputStream,ObjectOutput}
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
case class SortingKey(columns: Array[Byte], ids: Identities, index: Long)

object SortingKeyComparator {
  final val serialVersionUID = 20120730l

  def apply(ascending: Boolean) = new SortingKeyComparator(ascending)
}
  
class SortingKeyComparator private[SortingKeyComparator] (ascending: Boolean) extends Comparator[SortingKey] with Serializable {
  def compare(a: SortingKey, b: SortingKey) = {
    // retrieve the selector, type and value for each column in the keys, grouped by the selector
    val aValsRaw: Array[(String,CValue)] = ColumnCodec.readOnly.decodeWithRefs(a.columns)
    val aVals: Map[String,Array[(String,CValue)]] = aValsRaw.groupBy(_._1)
    val bVals: Map[String,Array[(String,CValue)]] = ColumnCodec.readOnly.decodeWithRefs(b.columns).groupBy(_._1)

    // Now, for each sort selector, compare in order based on comparable types
    var result = 0
    var i = 0

    val sortSelectors = aValsRaw.map(_._1).distinct

    while (result == 0 && i < sortSelectors.length) {
      val toCompare = (aVals(sortSelectors(i)).find(_._2 != CUndefined), bVals(sortSelectors(i)).find(_._2 != CUndefined))
      result = toCompare match {
        case (None, None)         => 0
        case (None, _)            => -1
        case (_, None)            => 1
        case (Some((_, av)), Some((_, bv))) => CValue.compareValues(av, bv)
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
  def apply(idCount: Int) = new SortingKeySerializer(idCount)
}

class SortingKeySerializer private[SortingKeySerializer](idCount: Int) extends Serializer[SortingKey] with Serializable {
  import CValueSerializerUtil.defaultSerializer

  final val serialVersionUID = 20120807l

  private[this] var idSerializer  = IdentitiesSerializer(idCount)

  def serialize(out: DataOutput, sk: SortingKey) {
    defaultSerializer.serialize(out, new java.lang.Long(sk.index))
    defaultSerializer.serialize(out, sk.columns)
    idSerializer.serialize(out, sk.ids)
  }

  def deserialize(in: DataInput): SortingKey = {
    val index = defaultSerializer.deserialize(in).asInstanceOf[java.lang.Long]
    SortingKey(defaultSerializer.deserialize(in).asInstanceOf[Array[Byte]], idSerializer.deserialize(in), index)
  }
}
 
