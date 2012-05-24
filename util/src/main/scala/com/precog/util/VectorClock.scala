package com.precog.util
  
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
