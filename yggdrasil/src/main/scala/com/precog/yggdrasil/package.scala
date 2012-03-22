package com.precog

import scalaz.{Order,Ordering}
import scalaz.effect.IO

import java.io.File

import com.precog.common.VectorCase

package object yggdrasil {
  type ProjectionDescriptorIO = ProjectionDescriptor => IO[Unit] 
  type ProjectionDescriptorLocator = ProjectionDescriptor => IO[File]
  
  type Identity = Long
  type Identities = VectorCase[Identity]
  type SEvent = (Identities, SValue)
  type SColumn = (Identities, CValue)

  object Identities {
    def Empty = VectorCase.empty[Identity]
  }

  object SEvent {
    def apply(id: Identities, sv: SValue): SEvent = (id, sv)
  }

  //TODO: should this not just be an Order[Identities]
  implicit def identityOrder(ids1: Identities, ids2: Identities): Ordering = 
    prefixIdentityOrder(ids1, ids2, ids1.length min ids2.length)

  //TODO: This makes no sense as an implicit function
  def prefixIdentityOrder(ids1: Identities, ids2: Identities, prefixLength: Int): Ordering = {
    var result: Ordering = Ordering.EQ
    var i = 0
    while (i < prefixLength && (result eq Ordering.EQ)) {
      val i1 = ids1(i)
      val i2 = ids2(i)
      
      if (i1 != i2) {
        result = Ordering.fromInt((i1 - i2) toInt)
      }
      
      i += 1
    }
    
    result
  }

  //TODO: This should use Order#contramap
  implicit def combinedIdentitiesOrder[A, B](p1: (Identities, A), p2: (Identities, B)): Ordering = {
    identityOrder(p1._1, p2._1)
  }

  implicit object IdentitiesOrder extends Order[Identities] {
    def order(i1: Identities, i2: Identities) = identityOrder(i1, i2)
  }

  implicit val IdentitiesOrdering = IdentitiesOrder.toScalaOrdering

  object SEventIdentityOrder extends Order[SEvent] {
    def order(s1: SEvent, s2: SEvent) = identityOrder(s1._1, s2._1)
  }
}



// vim: set ts=4 sw=4 et:
