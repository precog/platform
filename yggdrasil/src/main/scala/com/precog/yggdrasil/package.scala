package com.precog

import scalaz.{Order,Ordering}
import scalaz.effect.IO
import scalaz.Ordering._
import scalaz.std.anyVal._

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
