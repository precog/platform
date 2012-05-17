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
package com.precog.common.util
  
import blueeyes.json.JsonAST._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import scalaz.Validation
import scalaz.Order
import scalaz.Ordering._

case class VectorClock(map: Map[Int, Int]) {   
  def get(id: Int): Option[Int] = map.get(id)  
  def hasId(id: Int): Boolean = map.contains(id)

  def update(id: Int, sequence: Int): VectorClock = 
    if (map.get(id) forall { _ <= sequence }) {
      VectorClock(map + (id -> sequence))
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
}


// vim: set ts=4 sw=4 et:
