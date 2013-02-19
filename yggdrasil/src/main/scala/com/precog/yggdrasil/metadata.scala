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

import com.precog.common._

import scalaz.Monoid
import scala.collection.mutable

package object metadata {
  type MetadataMap = Map[MetadataType, Metadata]
  
  type ColumnMetadata = Map[ColumnRef, MetadataMap]

  object ColumnMetadata {
    val Empty = Map.empty[ColumnRef, MetadataMap]

    implicit object monoid extends Monoid[ColumnMetadata] {
      val zero = Empty

      def append(m1: ColumnMetadata, m2: => ColumnMetadata): ColumnMetadata = {
        m1.foldLeft(m2) {
          case (acc, (descriptor, mmap)) =>
            val currentMmap: MetadataMap = acc.getOrElse(descriptor, Map.empty[MetadataType, Metadata])
            val newMmap: MetadataMap = mmap.foldLeft(currentMmap) {
              case (macc, (mtype, m)) => 
                macc + (mtype -> macc.get(mtype).flatMap(_.merge(m)).getOrElse(m))
            }

            acc + (descriptor -> newMmap)
        }
      }
    }
  }
}
