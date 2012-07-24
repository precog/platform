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
