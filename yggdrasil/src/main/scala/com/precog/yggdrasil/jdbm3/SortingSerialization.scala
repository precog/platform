package com.precog.yggdrasil
package jdbm3

import org.apache.jdbm.Serializer

import java.io.{DataInput,DataOutput,Externalizable,ObjectInput,ObjectInputStream,ObjectOutput}
import java.util.Comparator

import scala.collection.BitSet

object SortingKeyComparator {
  final val serialVersionUID = 20120730l

  def apply(rowFormat: RowFormat, ascending: Boolean) = new SortingKeyComparator(rowFormat, ascending)
}

class SortingKeyComparator private[SortingKeyComparator] (rowFormat: RowFormat, ascending: Boolean)
    extends Comparator[Array[Byte]] with Serializable {

  // TODO: Optimize by going directly to selector-driven ordered raw values (or undefined)
  def compare(a: Array[Byte], b: Array[Byte]) = rowFormat.compare(a, b)
}
