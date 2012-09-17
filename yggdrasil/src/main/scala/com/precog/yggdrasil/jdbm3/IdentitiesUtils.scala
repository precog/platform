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
    val ret = a.zip(b).dropWhile { case (x,y) => x == y }.headOption.map {
      case (x,y) => (x - y).signum
    }.getOrElse(a.length - b.length)

    if (ascending) ret else -ret
  }
}

object AscendingIdentitiesComparator extends IdentitiesComparator(true)
