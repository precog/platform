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
package com.precog

import scalaz.{Order,Ordering}
import scalaz.effect.IO
import scalaz.Ordering._
import scalaz.std.anyVal._

import java.io.File

import com.precog.common.VectorCase
import com.precog.util.PrecogUnit

package object yggdrasil {
  type ProjectionDescriptorIO = ProjectionDescriptor => IO[PrecogUnit] 
  type ProjectionDescriptorLocator = ProjectionDescriptor => IO[File]
  
  type Identity = Long
  type Identities = Array[Identity]
  type SEvent = (Identities, SValue)

  object Identities {
    val Empty = VectorCase.empty[Identity]
  }

  object SEvent {
    @inline 
    def apply(id: Identities, sv: SValue): SEvent = (id, sv)
  }

  def prefixIdentityOrdering(ids1: Identities, ids2: Identities, prefixLength: Int): Ordering = {
    var result: Ordering = EQ
    var i = 0
    while (i < prefixLength && (result eq EQ)) {
      result = longInstance.order(ids1(i), ids2(i))
      i += 1
    }
    
    result
  }

  def fullIdentityOrdering(ids1: Identities, ids2: Identities) = prefixIdentityOrdering(ids1, ids2, ids1.length min ids2.length)

  object IdentitiesOrder extends Order[Identities] {
    def order(ids1: Identities, ids2: Identities) = fullIdentityOrdering(ids1, ids2)
  }

  def prefixIdentityOrder(prefixLength: Int): Order[Identities] = {
    new Order[Identities] {
      def order(ids1: Identities, ids2: Identities) = prefixIdentityOrdering(ids1, ids2, prefixLength)
    }
  }

  def indexedIdentitiesOrder(indices: Vector[Int]): Order[Identities] = {
    new Order[Identities] {
      def order(ids1: Identities, ids2: Identities): Ordering = {
        var result: Ordering = EQ
        var i = 0
        while (i < indices.length && (result eq EQ)) {
          result = longInstance.order(ids1(indices(i)), ids2(indices(i)))
          i += 1
        }

        result
      }
    }
  }

  def tupledIdentitiesOrder[A](idOrder: Order[Identities] = IdentitiesOrder): Order[(Identities, A)] =
    idOrder.contramap((_: (Identities, A))._1)

  def identityValueOrder[A](idOrder: Order[Identities] = IdentitiesOrder)(implicit ord: Order[A]): Order[(Identities, A)] = new Order[(Identities, A)] {
    type IA = (Identities, A)
    def order(x: IA, y: IA): Ordering = {
      val idComp = idOrder.order(x._1, y._1)
      if (idComp == EQ) {
        ord.order(x._2, y._2)
      } else idComp
    }
  }

  def valueOrder[A](implicit ord: Order[A]): Order[(Identities, A)] = new Order[(Identities, A)] {
    type IA = (Identities, A)
    def order(x: IA, y: IA): Ordering = {
      ord.order(x._2, y._2)
    }
  }
}

// vim: set ts=4 sw=4 et:
