package com.precog.yggdrasil
package jdbm3

import org.apache.jdbm.Serializer

import java.io.{DataInput,DataOutput}
import java.util.Comparator

import com.precog.common.{Vector0,Vector1,Vector2,Vector3,Vector4,VectorCase}

object IdentitiesComparator {
  private final val serialVersionUID = 20120724l

  def apply(ascending: Boolean) = new IdentitiesComparator(ascending)
}

class IdentitiesComparator private[jdbm3](val ascending: Boolean) extends Comparator[Identities] with Serializable {
  def compare (a: Identities, b: Identities) = {
    val len = if (a.length < b.length) a.length else b.length

    var i = 0
    var cmp = 0
    while (cmp != 0 && i < len) {
      val x = a(i)
      val y = b(i)
      cmp = if (x < y) -1 else if (x == y) 0 else 1
      i += 1
    }

    cmp = if (cmp != 0) cmp else a.length - b.length
    if (ascending) cmp else -cmp
  }
}

object AscendingIdentitiesComparator extends IdentitiesComparator(true)
