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
