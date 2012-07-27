package com.precog.yggdrasil
package jdbm3

import org.apache.jdbm.Serializer

import java.io.{DataInput,DataOutput}
import java.util.Comparator

import com.precog.common.{Vector0,Vector1,Vector2,Vector3,Vector4,VectorCase}

object IdentitiesComparator extends IdentitiesComparator {
  final val serialVersionUID = 20120724l
}

class IdentitiesComparator extends Comparator[Identities] with Serializable {
  def readResolve() = IdentitiesComparator

  def compare (a: Identities, b: Identities) = {
    a.zip(b).dropWhile { case (x,y) => x == y }.headOption.map {
      case (x,y) => (x - y).signum
    }.getOrElse(a.length - b.length)
  }
}

object IdentitiesSerializer extends IdentitiesSerializer {
  final val serialVersionUID = 20120724l
}

class IdentitiesSerializer extends Serializer[Identities] with Serializable {
  def readResolve() = IdentitiesSerializer

  def serialize(out: DataOutput, ids: Identities) {
    out.writeInt(ids.size)
    ids.foreach { i => out.writeLong(i) }
  }

  def deserialize(in: DataInput): Identities = {
    in.readInt() match {
      case 0 => Vector0
      case 1 => Vector1(in.readLong())
      case 2 => Vector2(in.readLong(), in.readLong())
      case 3 => Vector3(in.readLong(), in.readLong(), in.readLong())
      case 4 => Vector4(in.readLong(), in.readLong(), in.readLong(), in.readLong())
      case length => {
        val tmp = new Array[Long](length)
        var i = 0
        while (i < length) { tmp(i) = in.readLong(); i += 1 }
        VectorCase(tmp: _*)
      }
    }
  } 
}
