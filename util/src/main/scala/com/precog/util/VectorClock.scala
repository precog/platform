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
package com.precog.util
  
import blueeyes.json.JsonAST._

import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import scalaz.Validation
import scalaz.Order
import scalaz.Semigroup
import scalaz.Ordering._

case class VectorClock(map: Map[Int, Int]) {   
  def get(producerId: Int): Option[Int] = map.get(producerId)  
  def hasId(producerId: Int): Boolean = map.contains(producerId)

  def update(producerId: Int, sequenceId: Int): VectorClock = 
    if (map.get(producerId) forall { _ <= sequenceId }) {
      VectorClock(map + (producerId -> sequenceId))
    } else {
      this 
    }

  def isDominatedBy(other: VectorClock): Boolean = map forall { 
    case (prodId, maxSeqId) => other.get(prodId).forall(_ >= maxSeqId)
  }
}

trait VectorClockSerialization {
  implicit val VectorClockDecomposer: Decomposer[VectorClock] = new Decomposer[VectorClock] {
    override def decompose(clock: VectorClock): JValue = clock.map.serialize 
  }

  implicit val VectorClockExtractor: Extractor[VectorClock] = new Extractor[VectorClock] with ValidatedExtraction[VectorClock] {
    override def validated(obj: JValue): Validation[Error, VectorClock] = 
      (obj.validated[Map[Int, Int]]).map(VectorClock(_))
  }
}

object VectorClock extends VectorClockSerialization {
  def empty = apply(Map.empty)

  implicit object order extends Order[VectorClock] {
    def order(c1: VectorClock, c2: VectorClock) = 
      if (c2.isDominatedBy(c1)) {
        if (c1.isDominatedBy(c2)) EQ else GT
      } else {
        LT
      }
  }

  // Computes the maximal merge of two clocks
  implicit object semigroup extends Semigroup[VectorClock] {
    def append(c1: VectorClock, c2: => VectorClock) = {
      c2.map.foldLeft(c1) { 
        case (acc, (producerId, sequenceId)) => acc.update(producerId, sequenceId)
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
