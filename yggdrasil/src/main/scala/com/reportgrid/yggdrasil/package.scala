package com.reportgrid

package object yggdrasil {
  type Identity = Long
  type Identities = Seq[Identity]
  type SEvent = (Identities, SValue)

  object SEvent {
    def apply(id: Identities, sv: SValue): SEvent = (id, sv)
  }

  implicit object IdentitiesOrdering extends Ordering[Identities] {
    def compare(id1: Identities, id2: Identities) = {
      (id1 zip id2).foldLeft(0) {
        case (acc, (i1, i2)) => if (acc != 0) acc else implicitly[Ordering[Identity]].compare(i1, i2)
      }
    }
  }
}



// vim: set ts=4 sw=4 et:
