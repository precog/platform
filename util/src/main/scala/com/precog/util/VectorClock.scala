package com.precog.util
  
import blueeyes.json.JsonAST._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

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
